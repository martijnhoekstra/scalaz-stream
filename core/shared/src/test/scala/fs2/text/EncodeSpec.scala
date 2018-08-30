package fs2

//import org.scalacheck._
import fs2.text._

//import TestUtil._

class EncodeSpec extends Fs2Spec {
  "text" - {
    "utf8Encoder" - {
      "terminates" in forAll { (str: String) => {
        val l = Stream(str).through(utf8Encode).toList
        l.mkString.isEmpty shouldBe (str == "")
      }}
      

      "encodes A" in {
        Stream("A").through(utf8Encode).toList shouldBe List(65)
      }
    }
  }
}