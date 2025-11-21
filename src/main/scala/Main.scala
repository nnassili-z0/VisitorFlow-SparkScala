package test.apachesparkscala.visitorflow

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

/**
  * Pipeline Spark de base pour capturer et analyser des parcours utilisateurs à partir d'événements web (clics, scrolls, vues de page, etc.).
  *
  * Utilisation (exemples):
  *
  * sbt run -- --source=files --inputPath=./data/events --outputPath=./out --checkpointPath=./chk --sessionTimeoutMinutes=30
  *
  * sbt run -- --source=kafka --kafkaBrokers=localhost:9092 --kafkaTopic=webevents --outputPath=./out --checkpointPath=./chk
  *
  * Format d'entrée JSON attendu pour chaque événement (une ligne JSON par événement):
  * {
  *   "userId": "u123",
  *   "timestamp": 1732000000000,
  *   "eventType": "click|view|scroll|mousemove",
  *   "pageUrl": "https://exemple.com/page",
  *   "referrer": "https://exemple.com/",
  *   "element": "#css-selector OU xpath",
  *   "x": 123, "y": 456, "screenWidth": 1920, "screenHeight": 1080,
  *   "meta": { "any": "thing" }
  * }
  */
object Main {

  case class WebEvent(
      userId: String,
      timestamp: Long,
      eventType: String,
      pageUrl: String,
      referrer: String,
      element: String,
      x: java.lang.Integer,
      y: java.lang.Integer,
      screenWidth: java.lang.Integer,
      screenHeight: java.lang.Integer,
      meta: String // JSON string (brut) pour simplicité
  )

  private def eventSchema: StructType = StructType(Seq(
    StructField("userId", StringType, nullable = false),
    StructField("timestamp", LongType, nullable = false), // epoch millis
    StructField("eventType", StringType, nullable = false),
    StructField("pageUrl", StringType, nullable = true),
    StructField("referrer", StringType, nullable = true),
    StructField("element", StringType, nullable = true),
    StructField("x", IntegerType, nullable = true),
    StructField("y", IntegerType, nullable = true),
    StructField("screenWidth", IntegerType, nullable = true),
    StructField("screenHeight", IntegerType, nullable = true),
    StructField("meta", StringType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val argMap = parseArgs(args)

    val source = argMap.getOrElse("source", "files") // files | kafka
    val inputPath = argMap.getOrElse("inputPath", "./data/events")
    val outputPath = argMap.getOrElse("outputPath", "./out")
    val checkpointPath = argMap.getOrElse("checkpointPath", "./chk")
    val sessionTimeoutMinutes = argMap.get("sessionTimeoutMinutes").map(_.toInt).getOrElse(30)
    val kafkaBrokers = argMap.getOrElse("kafkaBrokers", "localhost:9092")
    // Sur certains environnements Windows/Docker, "localhost" peut se comporter différemment.
    // On ajoute 127.0.0.1 en secours pour la connexion Kafka côté client.
    val effectiveBrokers = if (kafkaBrokers == "localhost:9092") "localhost:9092,127.0.0.1:9092" else kafkaBrokers
    val kafkaTopic = argMap.getOrElse("kafkaTopic", "webevents")
    val isWindows = System.getProperty("os.name", "").toLowerCase.contains("win")

    // Fallback Windows sans Spark (évite les erreurs winutils.exe):
    // Active par défaut sur Windows, désactivable avec --forceSparkOnWindows=true
    val forceSparkOnWindows = argMap.getOrElse("forceSparkOnWindows", "false").toBoolean
    if (isWindows && !forceSparkOnWindows && source == "kafka") {
      println("[Windows Fallback] Running lightweight Kafka consumer analyzer (no Spark). To force Spark on Windows, pass --forceSparkOnWindows=true")
      ensureKafkaReadyAndTopic(effectiveBrokers, kafkaTopic, totalWaitSeconds = 120)
      val outDir = argMap.getOrElse("outputPath", "./out")
      runWindowsKafkaFallback(effectiveBrokers, kafkaTopic, sessionTimeoutMinutes, outDir)
      return
    }

    // Workaround Windows: éviter FileNotFoundException(HADOOP_HOME/hadoop.home.dir) côté Hadoop
    ensureHadoopHomeOnWindows()

    val spark = SparkSession.builder()
      .appName("UserJourneyPipeline")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      // Contournement Windows: désactiver la gestion des permissions Hadoop pour éviter l'appel à winutils.exe
      .config("spark.hadoop.fs.permissions.enabled", "false")
      // Forcer l'usage du système de fichiers local brut et éviter le cache d'impl
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      // Éviter le chargement des bibliothèques natives Hadoop (réduit les erreurs winutils)
      .config("spark.hadoop.io.native.lib.available", "false")
      // S'assurer que le FS par défaut est local
      .config("spark.hadoop.fs.defaultFS", "file:///")
      // Répertoires locaux explicites pour Spark afin d'éviter des chemins protégés sous Windows
      .config("spark.sql.warehouse.dir", new java.io.File(".spark-warehouse").getAbsolutePath)
      .config("spark.local.dir", new java.io.File(".spark-local").getAbsolutePath)
      // Forcer un CheckpointFileManager basé sur FileSystem, souvent plus tolérant sous Windows
      // NOTE: la clé correcte est `spark.sql.streaming.checkpointFileManagerClass`
      .config(
        "spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager"
      )
      // Éviter les dossiers de checkpoints temporaires persistants (et les permissions associées)
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", sys.env.getOrElse("SHUFFLE_PARTITIONS", "4"))

    // Si on lit depuis Kafka, assurons-nous que le broker est prêt et que le topic existe avant de démarrer le streaming
    if (source == "kafka") {
      ensureKafkaReadyAndTopic(effectiveBrokers, kafkaTopic, totalWaitSeconds = 120)
    }

    val raw: DataFrame = source match {
      case "files" =>
        spark.readStream
          .schema(eventSchema)
          .option("maxFilesPerTrigger", 1)
          .json(inputPath)
      case "kafka" =>
        val starting = argMap.getOrElse("kafkaStartingOffsets", "earliest") // "latest" ou "earliest"
        val kafkaDf = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", effectiveBrokers)
          .option("subscribe", kafkaTopic)
          .option("startingOffsets", starting)
          .option("failOnDataLoss", "false")
          .load()
        val valueStr = kafkaDf.selectExpr("CAST(value AS STRING) as json")
        // Parsing JSON en mode streaming via from_json + schéma explicite
        valueStr
          .select(from_json(col("json"), eventSchema).as("data"))
          .select("data.*")
      case other =>
        throw new IllegalArgumentException(s"source inconnue: $other (attendu: files|kafka)")
    }

    val events = raw
      .withColumn("eventTime", to_timestamp(from_unixtime(col("timestamp") / 1000)))
      .select(
        col("userId"), col("timestamp"), col("eventType"), col("pageUrl"), col("referrer"), col("element"),
        col("x"), col("y"), col("screenWidth"), col("screenHeight"), col("meta"), col("eventTime")
      )
      .withWatermark("eventTime", s"${math.max(1, sessionTimeoutMinutes)} minutes")

    // 1) Heatmap basique: agrégation par page/élément et buckets de position
    val heatmap = events
      .filter(col("eventType") === lit("click"))
      .withColumn("xBucket", (col("x")/50).cast("int")*50)
      .withColumn("yBucket", (col("y")/50).cast("int")*50)
      .groupBy(
        window(col("eventTime"), "10 minutes"),
        col("pageUrl"), col("element"), col("xBucket"), col("yBucket")
      ).count()

    // déjà déterminé ci-dessus
    // Ecriture heatmap:
    // - Windows (sans Spark forcé): on ne passe pas ici (fallback plus haut)
    // - Windows avec Spark forcé: écrire en Parquet
    // - Non-Windows: écrire en Parquet
    val heatmapWriterConsole = heatmap.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
    val heatmapWriterParquet = heatmap.writeStream
      .format("parquet")
      .option("path", s"$outputPath/heatmap")
      .option("checkpointLocation", s"$checkpointPath/heatmap")
      .outputMode("append")
    val heatmapQuery = (
      if (isWindows && !forceSparkOnWindows) heatmapWriterConsole
      else heatmapWriterParquet
    ).start()

    // 2) Parcours (chemins) par user et session: session_window + séquence d'URLs
    val seqAgg = events
      .groupBy(
        col("userId"),
        session_window(col("eventTime"), s"${sessionTimeoutMinutes} minutes")
      )
      .agg(
        sort_array(collect_list(struct(col("eventTime"), col("pageUrl")))).as("pages")
      )
      .withColumn("path", expr("transform(pages, x -> x.pageUrl)"))
      .select(col("userId"), col("session_window.start").as("sessionStart"), col("session_window.end").as("sessionEnd"), col("path"))

    val pathWriterNonWin = seqAgg.writeStream
      .format("parquet")
      .option("path", s"$outputPath/paths")
      .option("checkpointLocation", s"$checkpointPath/paths")
      .outputMode("append")
    val pathWriterWin = seqAgg.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("append")
    val pathQuery = (if (isWindows) pathWriterWin else pathWriterNonWin).start()

    // 3) Erreurs/perf simples: compter les events de type "error" et latences simulées si fournies
    val errors = events.filter(col("eventType") === "error")
      .groupBy(window(col("eventTime"), "5 minutes"), col("pageUrl"))
      .count()

    val errorWriter = errors.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
    val errorQuery = (if (isWindows) errorWriter else errorWriter.option("checkpointLocation", s"$checkpointPath/errors")).start()

    spark.streams.awaitAnyTermination()
    // Stop explicite si une query termine (peu probable en continu)
    heatmapQuery.stop(); pathQuery.stop(); errorQuery.stop()
  }

  def parseArgs(args: Array[String]): Map[String, String] = {
    args.toList
      .filter(_.startsWith("--"))
      .map(_.drop(2))
      .flatMap { kv =>
        kv.split("=", 2) match {
          case Array(k, v) => Some(k -> v)
          case Array(k) if k.nonEmpty => Some(k -> "true")
          case _ => None
        }
      }.toMap
  }

  /**
    * Attend que Kafka réponde et s'assure que le topic existe (création si manquant).
    * Ceci évite les erreurs au démarrage lorsque Docker Kafka n'est pas encore prêt
    * au moment où Spark essaie de s'abonner.
    */
  private def ensureKafkaReadyAndTopic(brokers: String, topic: String, totalWaitSeconds: Int = 60): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)

    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(totalWaitSeconds.toLong)
    var created = false

    def remainingMs: Long = math.max(0L, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()))

    println(s"[Kafka] Attente du broker $brokers et du topic '$topic' (timeout ${totalWaitSeconds}s)…")
    while (System.nanoTime() < deadline) {
      var client: AdminClient = null
      try {
        client = AdminClient.create(props)
        val names = client.listTopics().names().get(2, TimeUnit.SECONDS)
        if (!names.contains(topic)) {
          // tenter création (idempotent si déjà créé par ailleurs)
          val nt = new NewTopic(topic, /*partitions*/ 1, /*replication*/ 1.toShort)
          try {
            client.createTopics(java.util.Collections.singleton(nt)).all().get(5, TimeUnit.SECONDS)
            created = true
          } catch {
            case _: Throwable => // peut déjà exister, ignorer
          }
        }
        // Revérifier la présence du topic
        val names2 = client.listTopics().names().get(2, TimeUnit.SECONDS)
        if (names2.contains(topic)) {
          if (created) println(s"[Kafka] Topic '$topic' prêt (créé)") else println(s"[Kafka] Topic '$topic' prêt")
          return
        }
      } catch {
        case _: Throwable =>
          // broker pas encore prêt
      } finally {
        if (client != null) client.close()
      }
      Thread.sleep(1000)
    }
    // Dernière tentative informative (ne pas planter l'appli, Spark essayera aussi et affichera les erreurs si besoin)
    println(s"[Kafka] Avertissement: impossible de valider la disponibilité de '$topic' sous $brokers après ${totalWaitSeconds}s. Tentative de démarrage quand même…")
  }

  /**
    * Mode fallback Windows: évite Spark Structured Streaming et lit Kafka directement.
    * Affiche en console un résumé simple des clics (heatmap buckets) et des erreurs par page.
    */
  private def runWindowsKafkaFallback(brokers: String, topic: String, sessionTimeoutMinutes: Int, outputPath: String): Unit = {
    import java.time.{Instant, ZoneId}
    import java.time.format.DateTimeFormatter
    import java.util.Properties
    import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
    import java.time.Duration
    import scala.jdk.CollectionConverters._
    import java.nio.file.{Files, Paths, StandardOpenOption}

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"fallback-consumer-${System.currentTimeMillis()}")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Arrays.asList(topic))

    @volatile var running = true
    Runtime.getRuntime.addShutdownHook(new Thread(() => { running = false; try consumer.wakeup() catch { case _: Throwable => () }; try consumer.close() catch { case _: Throwable => () } }))

    def field(json: String, name: String): Option[String] = {
      // extraction très simple: "name": value (string/number)
      val q = s"\"$name\""
      val idx = json.indexOf(q)
      if (idx < 0) None else {
        val colon = json.indexOf(':', idx + q.length)
        if (colon < 0) None else {
          val sub = json.substring(colon + 1).trim
          if (sub.startsWith("\"")) { // string
            val end = sub.indexOf('"', 1)
            if (end > 1) Some(sub.substring(1, end)) else None
          } else {
            // number or null until comma/brace
            val endCandidates = List(sub.indexOf(','), sub.indexOf('}')).filter(_ >= 0)
            val end = if (endCandidates.nonEmpty) endCandidates.min else sub.length
            Some(sub.substring(0, end).trim)
          }
        }
      }
    }

    case class HMKey(pageUrl: String, element: String, xBucket: Int, yBucket: Int)
    val heatmap = scala.collection.mutable.Map.empty[HMKey, Long].withDefaultValue(0L)
    val errorsByPage = scala.collection.mutable.Map.empty[String, Long].withDefaultValue(0L)
    val pathsByUser = scala.collection.mutable.Map.empty[String, scala.collection.mutable.ListBuffer[String]]

    val fmt = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault())
    val fileTsFmt = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneId.systemDefault())
    // Préparer les dossiers d'export JSON (./out/paths et ./out/heatmap par défaut)
    val pathsRoot = Paths.get(outputPath, "paths")
    val heatmapRoot = Paths.get(outputPath, "heatmap")
    try Files.createDirectories(pathsRoot) catch { case _: Throwable => () }
    try Files.createDirectories(heatmapRoot) catch { case _: Throwable => () }
    var lastReport = System.currentTimeMillis()
    println(s"[Windows Fallback] Connected to Kafka at $brokers, consuming topic '$topic'. Press Ctrl+C to stop.")
    while (running) {
      val records = consumer.poll(Duration.ofMillis(500))
      records.asScala.foreach { rec =>
        val json = rec.value()
        val eventType = field(json, "eventType").map(_.replace("\"", "")).getOrElse("")
        val pageUrl = field(json, "pageUrl").map(_.replace("\"", "")).getOrElse("")
        val element = field(json, "element").map(_.replace("\"", "")).getOrElse("body")
        val x = field(json, "x").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(0)
        val y = field(json, "y").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(0)
        val userId = field(json, "userId").getOrElse("?")

        if (eventType == "\"click\"" || eventType == "click") {
          val key = HMKey(pageUrl, element, (x/50)*50, (y/50)*50)
          heatmap.update(key, heatmap(key) + 1)
        }
        if (eventType == "\"error\"" || eventType == "error") {
          errorsByPage.update(pageUrl, errorsByPage(pageUrl) + 1)
        }
        val buf = pathsByUser.getOrElseUpdate(userId, scala.collection.mutable.ListBuffer.empty[String])
        if (pageUrl.nonEmpty && (buf.isEmpty || buf.lastOption.getOrElse("") != pageUrl)) buf += pageUrl
      }

      val now = System.currentTimeMillis()
      if (now - lastReport >= 5000) {
        lastReport = now
        val ts = fmt.format(Instant.ofEpochMilli(now))
        println(s"\n[Windows Fallback][$ts] Snapshot last 5s:")
        // Heatmap top 10
        val topHM = heatmap.toSeq.sortBy(-_._2).take(10)
        if (topHM.nonEmpty) {
          println("Heatmap top (page, element, xBucket, yBucket) -> count:")
          topHM.foreach { case (k, c) => println(s"  ${k.pageUrl} ${k.element} (${k.xBucket},${k.yBucket}) -> $c") }
        } else println("Heatmap: no clicks yet")
        // Errors
        if (errorsByPage.nonEmpty) {
          println("Errors by page:")
          errorsByPage.toSeq.sortBy(-_._2).foreach { case (p, c) => println(s"  $p -> $c") }
        } else println("Errors: none")
        // Paths (preview per user)
        if (pathsByUser.nonEmpty) {
          println("Paths preview (per user):")
          pathsByUser.take(5).foreach { case (u, path) => println(s"  $u: ${path.mkString(" -> ")}") }
          // Ecrire un snapshot JSONL sous out/paths/paths-YYYYMMDD-HHmmss.jsonl
          val snapName = s"paths-${fileTsFmt.format(Instant.ofEpochMilli(now))}.jsonl"
          val file = pathsRoot.resolve(snapName)
          val bldr = new StringBuilder
          pathsByUser.foreach { case (u, path) =>
            val pathEscaped = path.map(p => p.replace("\\", "\\\\").replace("\"", "\\\"") )
            bldr.append("{")
            bldr.append("\"userId\":\"").append(u.replace("\\", "\\\\").replace("\"", "\\\"")).append("\",")
            bldr.append("\"path\":[")
            var first = true
            pathEscaped.foreach { p =>
              if (!first) bldr.append(',') else first = false
              bldr.append('"').append(p).append('"')
            }
            bldr.append("]}")
            bldr.append(System.lineSeparator())
          }
          val bytes = bldr.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)
          try {
            Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
          } catch {
            case e: Throwable => println(s"[Windows Fallback] WARN: impossible d'écrire ${file.toAbsolutePath}: ${e.getMessage}")
          }
        }

        // Heatmap snapshot JSONL: une ligne par (pageUrl, element, xBucket, yBucket)
        if (heatmap.nonEmpty) {
          val hmSnapName = s"heatmap-${fileTsFmt.format(Instant.ofEpochMilli(now))}.jsonl"
          val hmFile = heatmapRoot.resolve(hmSnapName)
          val hb = new StringBuilder
          heatmap.foreach { case (k, c) =>
            def esc(s: String) = s.replace("\\", "\\\\").replace("\"", "\\\"")
            hb.append('{')
            hb.append("\"pageUrl\":\"").append(esc(k.pageUrl)).append("\",")
            hb.append("\"element\":\"").append(esc(k.element)).append("\",")
            hb.append("\"xBucket\":").append(k.xBucket).append(',')
            hb.append("\"yBucket\":").append(k.yBucket).append(',')
            hb.append("\"count\":").append(c)
            hb.append('}')
            hb.append(System.lineSeparator())
          }
          val hbytes = hb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)
          try {
            Files.write(hmFile, hbytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
          } catch {
            case e: Throwable => println(s"[Windows Fallback] WARN: impossible d'écrire ${hmFile.toAbsolutePath}: ${e.getMessage}")
          }
        }
      }
    }
    println("[Windows Fallback] Stopped.")
  }

  /**
    * Sous Windows, Hadoop attend que HADOOP_HOME ou la propriété système "hadoop.home.dir" soit défini.
    * On définit un dossier local discret s'il est absent pour éviter les erreurs bloquantes.
    */
  private def ensureHadoopHomeOnWindows(): Unit = {
    val os = System.getProperty("os.name", "").toLowerCase
    if (os.contains("win")) {
      val prop = System.getProperty("hadoop.home.dir")
      if (prop == null || prop.isEmpty) {
        val dir = new java.io.File(".hadoop")
        val bin = new java.io.File(dir, "bin")
        if (!bin.exists()) bin.mkdirs()
        System.setProperty("hadoop.home.dir", dir.getAbsolutePath)
        println(s"[Hadoop] hadoop.home.dir défini sur ${dir.getAbsolutePath}")
      }
    }
  }
}

