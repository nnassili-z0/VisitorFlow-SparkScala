package test.apachesparkscala.visitorflow

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Petit pont HTTP -> Kafka.
  * - POST /event avec un corps JSON: publie dans Kafka (topic configurable).
  * - Réponses CORS pour OPTIONS/POST.
  *
  * Usage:
  *   sbt bridge
  *   ou
  *   sbt "runMain test.apachesparkscala.visitorflow.HttpKafkaBridge -- --port=8080 --kafkaBrokers=localhost:9092 --kafkaTopic=webevents"
  */
object HttpKafkaBridge {
  final case class BridgeHandle(server: HttpServer, producer: KafkaProducer[String, String]) {
    def stop(): Unit = {
      try server.stop(0) finally producer.close()
    }
  }

  /** Démarre le serveur HTTP du pont et renvoie un handle pour l'arrêter. */
  def start(port: Int, brokers: String, topic: String): BridgeHandle = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.createContext("/event", new EventHandler(producer, topic))
    server.createContext("/health", new HealthHandler())
    // UI statique minimale: sert les fichiers depuis ./web
    server.createContext("/static", new StaticFilesHandler(baseDir = "web"))
    server.createContext("/", new RootHandler(baseDir = "web"))
    server.setExecutor(null)
    println(s"HttpKafkaBridge prêt sur http://localhost:$port (topic=$topic, brokers=$brokers)")
    server.start()
    BridgeHandle(server, producer)
  }

  def main(args: Array[String]): Unit = {
    val argMap = Main.parseArgs(args)
    val port = argMap.getOrElse("port", "8080").toInt
    val brokers = argMap.getOrElse("kafkaBrokers", "localhost:9092")
    val topic = argMap.getOrElse("kafkaTopic", "webevents")
    val handle = start(port, brokers, topic)
    // Empêche la sortie immédiate si lancé via "sbt bridge"
    // Attendre jusqu'à Ctrl+C, puis arrêter proprement
    val latch = new java.util.concurrent.CountDownLatch(1)
    Runtime.getRuntime.addShutdownHook(new Thread(() => { handle.stop(); latch.countDown() }))
    latch.await()
  }

  private class HealthHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      withCors(exchange)
      val bytes = "OK".getBytes(StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(200, bytes.length)
      val os = exchange.getResponseBody
      os.write(bytes)
      os.close()
    }
  }

  private class EventHandler(producer: KafkaProducer[String, String], topic: String) extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        exchange.getRequestMethod match {
          case "OPTIONS" =>
            withCors(exchange)
            exchange.sendResponseHeaders(204, -1)
          case "POST" =>
            val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
            withCors(exchange)
            // publier tel quel
            producer.send(new ProducerRecord[String, String](topic, null, body))
            val resp = "{\"status\":\"ok\"}"
            val bytes = resp.getBytes(StandardCharsets.UTF_8)
            exchange.sendResponseHeaders(200, bytes.length)
            val os = exchange.getResponseBody
            os.write(bytes)
            os.close()
          case _ =>
            withCors(exchange)
            exchange.sendResponseHeaders(405, -1)
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          val json = "{\"error\":\"" + Option(e.getMessage).getOrElse("internal error").replace("\"", "'") + "\"}"
          val bytes = json.getBytes(StandardCharsets.UTF_8)
          withCors(exchange)
          exchange.sendResponseHeaders(500, bytes.length)
          val os = exchange.getResponseBody
          os.write(bytes)
          os.close()
      }
    }
  }

  private def withCors(exchange: HttpExchange): Unit = {
    val headers = exchange.getResponseHeaders
    headers.add("Access-Control-Allow-Origin", "*")
    headers.add("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS")
    headers.add("Access-Control-Allow-Headers", "Content-Type")
  }

  // Sert / → sandbox.html si présent, sinon 404
  private class RootHandler(baseDir: String) extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        exchange.getRequestMethod match {
          case "OPTIONS" =>
            withCors(exchange); exchange.sendResponseHeaders(204, -1)
          case "GET" | "HEAD" =>
            val index = new java.io.File(baseDir, "sandbox.html")
            if (index.exists() && index.isFile) {
              StaticFilesHandler.sendFile(exchange, index)
            } else {
              withCors(exchange)
              exchange.sendResponseHeaders(404, -1)
            }
          case _ =>
            withCors(exchange); exchange.sendResponseHeaders(405, -1)
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          val json = "{\"error\":\"" + Option(e.getMessage).getOrElse("internal error").replace("\"", "'") + "\"}"
          val bytes = json.getBytes(StandardCharsets.UTF_8)
          withCors(exchange)
          exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
          exchange.sendResponseHeaders(500, bytes.length)
          val os = exchange.getResponseBody
          os.write(bytes); os.close()
      }
    }
  }

  // Sert les fichiers sous /static/* depuis baseDir
  private class StaticFilesHandler(baseDir: String) extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        exchange.getRequestMethod match {
          case "OPTIONS" =>
            withCors(exchange); exchange.sendResponseHeaders(204, -1)
          case "GET" | "HEAD" =>
            val path = Option(exchange.getRequestURI.getPath).getOrElse("/static")
            val sub = path.stripPrefix("/static").stripPrefix("/")
            val safe = sub.replace("..", "") // prévention simple de traversal
            val file = new java.io.File(baseDir, safe)
            if (file.exists() && file.isFile) {
              StaticFilesHandler.sendFile(exchange, file)
            } else {
              withCors(exchange); exchange.sendResponseHeaders(404, -1)
            }
          case _ =>
            withCors(exchange); exchange.sendResponseHeaders(405, -1)
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          val json = "{\"error\":\"" + Option(e.getMessage).getOrElse("internal error").replace("\"", "'") + "\"}"
          val bytes = json.getBytes(StandardCharsets.UTF_8)
          withCors(exchange)
          exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
          exchange.sendResponseHeaders(500, bytes.length)
          val os = exchange.getResponseBody
          os.write(bytes); os.close()
      }
    }
  }

  private object StaticFilesHandler {
    private val mimeByExt: Map[String, String] = Map(
      ".html" -> "text/html; charset=utf-8",
      ".htm"  -> "text/html; charset=utf-8",
      ".js"   -> "application/javascript; charset=utf-8",
      ".css"  -> "text/css; charset=utf-8",
      ".json" -> "application/json; charset=utf-8",
      ".map"  -> "application/json; charset=utf-8",
      ".png"  -> "image/png",
      ".jpg"  -> "image/jpeg",
      ".jpeg" -> "image/jpeg",
      ".svg"  -> "image/svg+xml"
    )

    private def mimeOf(file: java.io.File): String = {
      val name = file.getName.toLowerCase
      mimeByExt.collectFirst { case (ext, ct) if name.endsWith(ext) => ct }
        .getOrElse("application/octet-stream")
    }

    def sendFile(exchange: HttpExchange, file: java.io.File): Unit = {
      withCors(exchange)
      val method = exchange.getRequestMethod
      val headers = exchange.getResponseHeaders
      headers.add("Content-Type", mimeOf(file))
      val bytes = java.nio.file.Files.readAllBytes(file.toPath)
      if (method == "HEAD") {
        exchange.sendResponseHeaders(200, -1)
      } else {
        exchange.sendResponseHeaders(200, bytes.length)
        val os = exchange.getResponseBody
        os.write(bytes); os.close()
      }
    }
  }
}
