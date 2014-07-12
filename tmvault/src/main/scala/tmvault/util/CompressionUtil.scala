package tmvault.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.{NoSuchAlgorithmException, MessageDigest}
import java.util.zip._

import scala.annotation.tailrec

object CompressionUtil {

  def gzip(data: Array[Byte]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val compressor = new GZIPOutputStream(out)
    compressor.write(data)
    compressor.close()
    out.toByteArray()
  }

  def gunzip(data: Array[Byte]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val decompressor = new GZIPInputStream(new ByteArrayInputStream(data))
    val buffer = new Array[Byte](1024)
    @tailrec
    def copy() : Unit = {
      val n = decompressor.read(buffer)
      if (n >= 0) {
        out.write(buffer, 0, n)
        copy()
      }
    }
    copy()
    decompressor.close()
    out.toByteArray()
  }

  def deflate(data: Array[Byte]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val compressor = new DeflaterOutputStream(out)
    compressor.write(data)
    compressor.close()
    out.toByteArray()
  }

  def inflate(data: Array[Byte]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val decompressor = new InflaterInputStream(new ByteArrayInputStream(data))
    val buffer = new Array[Byte](1024)
    @tailrec
    def copy() : Unit = {
      val n = decompressor.read(buffer)
      if (n >= 0) {
        out.write(buffer, 0, n)
        copy()
      }
    }
    copy()
    decompressor.close()
    out.toByteArray()
  }

  def deflate2(data: Array[Byte]) : Array[Byte] = {
    val (compressor, outputBuffer) = threadLocalDeflater.get()
    compressor.reset()
    compressor.setInput(data)
    compressor.finish()
    val count = compressor.deflate(outputBuffer)
    if(!compressor.finished())
      throw new IndexOutOfBoundsException(s"Buffer size ${outputBuffer.length} to small to deflate data of size ${data.length}!")
    outputBuffer.take(count)
  }

  def inflate2(data: Array[Byte]) : Array[Byte] = {
    val (decompressor, outputBuffer) = threadLocalInflater.get()
    decompressor.reset()
    decompressor.setInput(data)
    val count = decompressor.inflate(outputBuffer)
    if(!decompressor.finished())
      throw new IndexOutOfBoundsException(s"Buffer size ${outputBuffer.length} to small to inflate data of size ${data.length}!")
    outputBuffer.take(count)
  }

  private final val threadLocalDeflater: ThreadLocal[(Deflater, Array[Byte])] = new ThreadLocal[(Deflater, Array[Byte])] {
    protected override def initialValue = {
      val deflater = new Deflater()
      deflater.setLevel(Deflater.DEFAULT_COMPRESSION)
      deflater.setStrategy(Deflater.DEFAULT_STRATEGY)
      (deflater, new Array[Byte](100000))
    }
  }

  private final val threadLocalInflater: ThreadLocal[(Inflater, Array[Byte])] = new ThreadLocal[(Inflater, Array[Byte])] {
    protected override def initialValue = (new Inflater(), new Array[Byte](100000))
  }
}