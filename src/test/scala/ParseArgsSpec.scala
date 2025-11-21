package test.apachesparkscala.contentsquare

import org.scalatest.funsuite.AnyFunSuite

class ParseArgsSpec extends AnyFunSuite {
  test("parseArgs should parse --k=v and --flag into a map") {
    val args = Array("--source=kafka", "--kafkaBrokers=localhost:9092", "--flag")
    val m = Main.parseArgs(args)
    assert(m("source") === "kafka")
    assert(m("kafkaBrokers") === "localhost:9092")
    assert(m("flag") === "true")
  }

  test("parseArgs should ignore entries without -- prefix") {
    val args = Array("run", "--a=1", "b=2")
    val m = Main.parseArgs(args)
    assert(m.contains("a"))
    assert(!m.contains("b"))
  }
}
