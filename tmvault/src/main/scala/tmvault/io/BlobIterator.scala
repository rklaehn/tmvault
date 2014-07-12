package tmvault.io

import java.nio.{ByteOrder, ByteBuffer}

class BlobIterator private(private val buffer: ByteBuffer) extends AnyVal {

  def hasRemaining = buffer.hasRemaining

  def get[T](implicit s: BlobSerializer[T]) = s.read(this)

  def getBoolean(): Boolean = buffer.get() != 0

  def getByte(): Byte = buffer.get()

  def getShort(): Short = buffer.getShort()

  def getInt(): Int = buffer.getInt()

  def getLong(): Long = buffer.getLong()

  def getFloat(): Float = buffer.getFloat()

  def getDouble(): Double = buffer.getDouble()

  def getChar(): Char = buffer.getChar()

  def getBooleans(size: Int): Array[Boolean] = {
    val result = new Array[Boolean](size)
    var i = 0
    while (i < result.length) {
      result(i) = getBoolean()
      i += 1
    }
    result
  }

  def getBooleans(): Array[Boolean] = getBooleans(buffer.remaining())

  def getBytes(size: Int): Array[Byte] = {
    val result = new Array[Byte](size)
    buffer.get(result)
    result
  }

  def getBytes(): Array[Byte] = getBytes(buffer.remaining())

  def getShorts(size: Int): Array[Short] = {
    val result = new Array[Short](size)
    buffer.asShortBuffer().get(result)
    buffer.position(buffer.position() + size * 2)
    result
  }

  def getShorts(): Array[Short] = getShorts(buffer.remaining() / 2)

  def getInts(size: Int): Array[Int] = {
    val result = new Array[Int](size)
    buffer.asIntBuffer().get(result)
    buffer.position(buffer.position() + size * 4)
    result
  }

  def getInts(): Array[Int] = getInts(buffer.remaining() / 4)

  def getLongs(size: Int): Array[Long] = {
    val result = new Array[Long](size)
    buffer.asLongBuffer().get(result)
    buffer.position(buffer.position() + size * 8)
    result
  }

  def getLongs(): Array[Long] = getLongs(buffer.remaining() / 8)

  def getFloats(size: Int): Array[Float] = {
    val result = new Array[Float](size)
    buffer.asFloatBuffer().get(result)
    buffer.position(buffer.position() + size * 4)
    result
  }

  def getFloats(): Array[Float] = getFloats(buffer.remaining() / 4)

  def getDoubles(size: Int): Array[Double] = {
    val result = new Array[Double](size)
    buffer.asDoubleBuffer().get(result)
    buffer.position(buffer.position() + size * 8)
    result
  }

  def getDoubles(): Array[Double] = getDoubles(buffer.remaining() / 8)

  def getChars(size: Int): Array[Char] = {
    val result = new Array[Char](size)
    buffer.asCharBuffer().get(result)
    buffer.position(buffer.position() + size * 2)
    result
  }

  def getChars(): Array[Char] = getChars(buffer.remaining() / 2)
}

object BlobIterator {

  def apply(source: Array[Byte]) = {
    val buffer = ByteBuffer.wrap(source)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    new BlobIterator(buffer)
  }
}
