package tmvault.util

import java.nio.charset.Charset
import java.nio.{ByteOrder, ByteBuffer}

import org.junit._

import scala.util.Random

class CompressionTest {

  def time[T](text:String)(action: => T) : (Double, T) = {
    val t0 = System.nanoTime()
    val result = action
    val dt = System.nanoTime() - t0
    println(text +" took " + dt / 1e9)
    (dt/ 1e9, result)
  }

  @Test
  def compressionSpeedTest() : Unit = {
    import CompressionUtil._
    val r = new Random(0)
    val sampleData = new String(Array.fill(32768/2)(r.nextPrintableChar()))
    println(sampleData)
    val bytes = sampleData.getBytes(Charset.forName("UTF-8"))
    var result = 0
    println(bytes.length)
    val (dt, _) = time("total") {
      for (i <- 0 until 100) {
        val n = 100
        time("deflate 1") {
          for (i <- 0 until n) {
            result += deflate(bytes).length
          }
        }
        time("deflate 2") {
          for (i <- 0 until n) {
            result += deflate2(bytes).length
          }
        }
      }
    }
    println("total bytes " + result)
    println("bytes per second " + (result / dt).toLong)
  }

  @Test
  def decompressionSpeedTest() : Unit = {
    import CompressionUtil._
    val r = new Random(0)
    val sampleData = new String(Array.fill(32768/2)(r.nextPrintableChar()))
    println(sampleData)
    val bytes = sampleData.getBytes(Charset.forName("UTF-8"))
    val compressed = deflate(bytes)
    var result = 0
    println(bytes.length)
    val (dt, _) = time("total") {
      for (i <- 0 until 100) {
        val n = 100
        time("inflate 1") {
          for (i <- 0 until n) {
            result += inflate(compressed).length
          }
        }
        time("inflate 2") {
          for (i <- 0 until n) {
            result += inflate2(compressed).length
          }
        }
      }
    }
    println("total bytes " + result)
    println("bytes per second " + (result / dt).toLong)
  }

  @Test
  def hashSpeedTest() : Unit = {
    val r = new Random(0)
    val sampleData = new String(Array.fill(32768/2)(r.nextPrintableChar()))
    println(sampleData)
    val bytes = sampleData.getBytes(Charset.forName("UTF-8"))
    var result = 0
    var count = 0
    println(bytes.length)
    val (dt, _) = time("total") {
      for (i <- 0 until 10000) {
        result += SHA1Hash.hash(bytes).hashCode
        count += bytes.length
      }
    }
    println("total bytes " + count)
    println("bytes per second " + (count / dt).toLong)
  }

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
