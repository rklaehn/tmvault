package tmvault.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
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
}