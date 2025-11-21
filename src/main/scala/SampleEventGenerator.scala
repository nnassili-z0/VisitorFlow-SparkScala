package test.apachesparkscala.visitorflow

import java.io.{BufferedWriter, File, FileWriter}
import java.time.Instant

/**
  * Génère quelques événements JSON d'exemple dans data/events pour tester la lecture "files".
  *
  * Usage:
  *   sbt genData
  */
object SampleEventGenerator {
  def main(args: Array[String]): Unit = {
    val outDir = new File("data/events")
    outDir.mkdirs()
    val now = System.currentTimeMillis()
    val file = new File(outDir, s"events-$now.json")
    val bw = new BufferedWriter(new FileWriter(file, /*append*/ true))
    try {
      val baseUrl = "http://localhost/sandbox"
      val evs =
        (0 until 5).flatMap { i =>
          val ts = now - (5000 - i * 500)
          Seq(
            json("u1", ts, "view", s"$baseUrl/page1", "", "body", nullInt, nullInt, 1280, 800),
            json("u1", ts + 50, "click", s"$baseUrl/page1", "", "#btn-cta", 300, 400, 1280, 800),
            json("u2", ts + 100, "view", s"$baseUrl/page2", "", "body", nullInt, nullInt, 1440, 900)
          )
        }
      evs.foreach { line =>
        bw.write(line)
        bw.newLine()
      }
      println(s"Ecrit ${evs.size} événements dans ${file.getAbsolutePath}")
    } finally {
      bw.close()
    }
  }

  private def nullInt: java.lang.Integer = null.asInstanceOf[java.lang.Integer]

  private def json(
      userId: String,
      timestamp: Long,
      eventType: String,
      pageUrl: String,
      referrer: String,
      element: String,
      x: java.lang.Integer,
      y: java.lang.Integer,
      screenWidth: java.lang.Integer,
      screenHeight: java.lang.Integer
  ): String = {
    val esc = (s: String) => s.replace("\"", "\\\"")
    s"""{
       "userId":"""" + esc(userId) + """",
       "timestamp":""" + timestamp + """,
       "eventType":""" + esc(eventType) + """",
       "pageUrl":""" + esc(pageUrl) + """",
       "referrer":""" + esc(referrer) + """",
       "element":""" + esc(element) + """",
       "x": """ + Option(x).map(_.toString).getOrElse("null") + """,
       "y": """ + Option(y).map(_.toString).getOrElse("null") + """,
       "screenWidth": """ + Option(screenWidth).map(_.toString).getOrElse("null") + """,
       "screenHeight": """ + Option(screenHeight).map(_.toString).getOrElse("null") + """,
       "meta": "{}"
     }""".stripMargin.replaceAll("\n", "").replaceAll("\\s+", " ").trim
  }
}
