package test.apachesparkscala.visitorflow

/**
  * Lance le pont HTTP→Kafka + l'appli Spark (lecture Kafka) dans le même JVM.
  *
  * Utilisation:
  *   sbt devUp
  *
  * Options (facultatives):
  *   --port=8080
  *   --kafkaBrokers=localhost:9092
  *   --kafkaTopic=webevents
  *   --outputPath=./out
  *   --checkpointPath=./chk
  */
object DevAll {
  def main(args: Array[String]): Unit = {
    val argMap = Main.parseArgs(args)
    val port = argMap.getOrElse("port", "8080").toInt
    val brokers = argMap.getOrElse("kafkaBrokers", "localhost:9092")
    val topic = argMap.getOrElse("kafkaTopic", "webevents")
    val outputPath = argMap.getOrElse("outputPath", "./out")
    val checkpointPath = argMap.getOrElse("checkpointPath", "./chk")
    val sessionTimeout = argMap.get("sessionTimeoutMinutes")

    // 1) Démarrer le pont
    val bridge = HttpKafkaBridge.start(port, brokers, topic)
    Runtime.getRuntime.addShutdownHook(new Thread(() => bridge.stop()))

    // 2) Lancer Spark en lecture Kafka (bloque jusqu'à Ctrl+C)
    val baseArgs = Array(
      "--source=kafka",
      s"--kafkaBrokers=$brokers",
      s"--kafkaTopic=$topic",
      s"--outputPath=$outputPath",
      s"--checkpointPath=$checkpointPath"
    )
    val sparkArgs = sessionTimeout match {
      case Some(mins) => baseArgs ++ Array(s"--sessionTimeoutMinutes=$mins")
      case None => baseArgs
    }
    Main.main(sparkArgs)
  }
}
