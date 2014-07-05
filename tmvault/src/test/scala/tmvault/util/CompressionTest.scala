package tmvault.util

import java.nio.{ByteOrder, ByteBuffer}

import org.junit._

import scala.util.Random

class CompressionTest {

  @Test
  def deltaCompressionTest(): Unit = {
    import CompressionUtil._

    def toByteArray(data: Array[Long]) = {
      val buffer = ByteBuffer
        .allocate(data.length * 8)
        .order(ByteOrder.LITTLE_ENDIAN)
      buffer.asLongBuffer().put(data)
      buffer.array()
    }

    def fromDeltas(data: Array[Long]): Array[Long] = {
      val result = new Array[Long](data.length)
      var x = data(0)
      result(0) = x
      var i = 1
      while (i < result.length) {
        x += data(i)
        result(i) = x
        i += 1
      }
      result
    }

    def toDeltas(data: Array[Long]): Array[Long] = {
      val result = new Array[Long](data.length)
      result(0) = data(0)
      var i = 0
      while (i < data.length - 1) {
        result(i + 1) = data(i + 1) - data(i)
        i += 1
      }
      result
    }

    val t0 = 1404422648192000000L
    val random = new Random(0)
    val times = {
      var t = t0
      Array.fill(4096) {
        t += 1000000 + random.nextInt(201) - 100
        t
      }
    }
    require(java.util.Arrays.equals(times, fromDeltas(toDeltas(times))))
    val bytes0 = toByteArray(times)
    val bytes1 = toByteArray(toDeltas(times))
    println(deflate(bytes0).length)
    println(deflate(bytes1).length)
  }
}
