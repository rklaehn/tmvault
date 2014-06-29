package tmvault.core

import java.nio.charset.Charset

class BlockBuilder private(var offset: Int, var data: Array[Byte]) {

  def bytes(bits: Int) =
    if ((bits & 7) == 0)
      (bits >>> 3)
    else
      (bits >>> 3) + 1

  def ensureCapacity(size: Int): Unit = {
    if (data.length < bytes(size)) {
      val data1 = new Array[Byte](data.length * 2)
      System.arraycopy(data, 0, data1, 0, data.length)
      data = data1
    }
  }

  def add(value: Boolean): BlockBuilder = {
    ensureCapacity(offset + 1)
    val byteOffset = offset >> 3
    data(byteOffset) = (data(byteOffset) | (1 << (offset & 7))).toByte
    offset += 1
    this
  }

  def addByte(value: Byte): BlockBuilder = {
    ensureCapacity(offset + 8)
    require((offset & 7) == 0)
    val byteOffset = offset >> 3
    data(byteOffset) = value
    offset += 8
    this
  }

  def add(value: Short): BlockBuilder = {
    ensureCapacity(offset + 16)
    require((offset & 7) == 0)
    val byteOffset = offset >> 3
    data(byteOffset + 0) = ((value >> 0) & 0xFF).toByte
    data(byteOffset + 1) = ((value >> 8) & 0xFF).toByte
    offset += 16
    this
  }

  def add(value: Int): BlockBuilder = {
    ensureCapacity(offset + 32)
    require((offset & 7) == 0)
    val byteOffset = offset >> 3
    data(byteOffset + 0) = ((value >> 0) & 0xFF).toByte
    data(byteOffset + 1) = ((value >> 8) & 0xFF).toByte
    data(byteOffset + 2) = ((value >> 16) & 0xFF).toByte
    data(byteOffset + 3) = ((value >> 24) & 0xFF).toByte
    offset += 32
    this
  }

  def add(value: Long): BlockBuilder = {
    ensureCapacity(offset + 64)
    require((offset & 7) == 0)
    val byteOffset = offset >> 3
    data(byteOffset + 0) = ((value >> 0) & 0xFF).toByte
    data(byteOffset + 1) = ((value >> 8) & 0xFF).toByte
    data(byteOffset + 2) = ((value >> 16) & 0xFF).toByte
    data(byteOffset + 3) = ((value >> 24) & 0xFF).toByte
    data(byteOffset + 4) = ((value >> 32) & 0xFF).toByte
    data(byteOffset + 5) = ((value >> 40) & 0xFF).toByte
    data(byteOffset + 6) = ((value >> 48) & 0xFF).toByte
    data(byteOffset + 7) = ((value >> 56) & 0xFF).toByte
    offset += 64
    this
  }

  def add(value:Array[Byte]): BlockBuilder = {
    ???
  }

  def add(value:Float): BlockBuilder = add(java.lang.Float.floatToIntBits(value))

  def add(value:Double): BlockBuilder = add(java.lang.Double.doubleToLongBits(value))

  def add(value:String): BlockBuilder = add(value.getBytes(BlockBuilder.UTF8))
}

object BlockBuilder {

  private val UTF8 = Charset.forName("UTF-8")
}