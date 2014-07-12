package tmvault.io

import java.nio.{ByteOrder, ByteBuffer}

/**
 * A wrapper for a byte buffer that is guaranteed to be in little endian byte order
 * @param buffer the wrapped ByteBuffer
 */
class BlobBuilder private(private val buffer: ByteBuffer) extends AnyVal {

  def put[T](value: T)(implicit s: BlobSerializer[T]) = s.write(value, this)

  def putBoolean(value: Boolean): Unit = buffer.put(if (value) 1.toByte else 0.toByte)

  def putByte(value: Byte): Unit = buffer.put(value)

  def putShort(value: Short): Unit = buffer.putShort(value)

  def putInt(value: Int): Unit = buffer.putInt(value)

  def putLong(value: Long): Unit = buffer.putLong(value)

  def putFloat(value: Float): Unit = buffer.putFloat(value)

  def putDouble(value: Double): Unit = buffer.putDouble(value)

  def putBooleans(value: Array[Boolean]): Unit = {
    var i = 0
    while (i < value.length) {
      putBoolean(value(i))
      i += 1
    }
  }

  def putBytes(value: Array[Byte]): Unit = buffer.put(value)

  def putShorts(value: Array[Short]): Unit = {
    buffer.asShortBuffer().put(value)
    buffer.position(buffer.position + value.length * 2)
  }

  def putInts(value: Array[Int]): Unit = {
    buffer.asIntBuffer().put(value)
    buffer.position(buffer.position + value.length * 4)
  }

  def putLongs(value: Array[Long]): Unit = {
    buffer.asLongBuffer().put(value)
    buffer.position(buffer.position + value.length * 8)
  }

  def putFloats(value: Array[Float]): Unit = {
    buffer.asFloatBuffer().put(value)
    buffer.position(buffer.position + value.length * 4)
  }

  def putDoubles(value: Array[Double]): Unit = {
    buffer.asDoubleBuffer().put(value)
    buffer.position(buffer.position + value.length * 8)
  }

  def putChars(value: Array[Char]): Unit = {
    buffer.asCharBuffer().put(value)
    buffer.position(buffer.position + value.length * 2)
  }

  def result = {
    // make sure nothing can be written to the buffer anymore
    buffer.limit(buffer.position)
    // return the underlying array
    buffer.array()
  }
}

object BlobBuilder {

  def apply(size: Int) = {
    val buffer = ByteBuffer.allocate(size)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    new BlobBuilder(buffer)
  }
}