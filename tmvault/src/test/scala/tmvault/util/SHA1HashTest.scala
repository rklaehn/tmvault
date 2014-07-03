package tmvault.util

import org.junit._

class SHA1HashTest {

  @Test
  def toStringParseRoundtripTest() : Unit = {
    val testData = (0 until 1000).map(_.toString.getBytes("UTF-8"))

    for(bytes <- testData) {
      val hash0 = SHA1Hash.hash(bytes)
      val text0 = hash0.toString
      val hash1 = SHA1Hash.parse(text0)
      assert(hash0 == hash1)
    }
  }
}
