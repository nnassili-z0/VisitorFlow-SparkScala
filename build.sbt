ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "VisitorFlow-SparkScala",
    idePackagePrefix := Some("test.apachesparkscala.visitorflow"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      // Producteur Kafka léger pour le pont HTTP -> Kafka
      "org.apache.kafka" % "kafka-clients" % "3.5.1",
      // Test unique avec ScalaTest
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),
    fork := true,
    // Compat JDK 17/21 avec Spark 3.5: ouvrir/exports modules nécessaires
    // (évite IllegalAccessError sun.nio.ch.DirectBuffer sous Java 21)
    javaOptions ++= Seq(
      "-Xmx2G",
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
  )

// Command aliases to avoid interactive selection ("choose a number") and make tests 1-2-3 straightforward
// Usage examples:
//   sbt genData
//   sbt streamFiles
//   sbt bridge
//   sbt streamKafka
addCommandAlias(
  "genData",
  "runMain test.apachesparkscala.visitorflow.SampleEventGenerator"
)
addCommandAlias(
  "streamFiles",
  "runMain test.apachesparkscala.visitorflow.Main -- --source=files --inputPath=./data/events --outputPath=./out --checkpointPath=./chk --sessionTimeoutMinutes=30"
)
addCommandAlias(
  "bridge",
  "runMain test.apachesparkscala.visitorflow.HttpKafkaBridge -- --port=8080 --kafkaBrokers=localhost:9092 --kafkaTopic=webevents"
)
addCommandAlias(
  "streamKafka",
  "runMain test.apachesparkscala.visitorflow.Main -- --source=kafka --kafkaBrokers=localhost:9092 --kafkaTopic=webevents --outputPath=./out --checkpointPath=./chk --sessionTimeoutMinutes=2"
)

addCommandAlias(
  "devUp",
  "runMain test.apachesparkscala.visitorflow.DevAll -- --port=8080 --kafkaBrokers=localhost:9092 --kafkaTopic=webevents --outputPath=./out --checkpointPath=./chk --sessionTimeoutMinutes=2"
)
