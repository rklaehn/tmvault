package tmvault.util

private[tmvault] object BitUtil {

  implicit class LittleEndianLong(private val value:Long) extends AnyVal {

    def byte7: Byte = (value >>> 56).toByte

    def byte6: Byte = (value >>> 48).toByte

    def byte5: Byte = (value >>> 40).toByte

    def byte4: Byte = (value >>> 32).toByte

    def byte3: Byte = (value >>> 24).toByte

    def byte2: Byte = (value >>> 16).toByte

    def byte1: Byte = (value >>> 8).toByte

    def byte0: Byte = (value >>> 0).toByte

    def copyToArray(target:Array[Byte], offset:Int) : Unit = {
      target(offset + 0) = byte0
      target(offset + 1) = byte1
      target(offset + 2) = byte2
      target(offset + 3) = byte3
      target(offset + 4) = byte4
      target(offset + 5) = byte5
      target(offset + 6) = byte6
      target(offset + 7) = byte7
    }
  }

  def byte7(x: Long): Byte = (x >> 56).toByte

  def byte6(x: Long): Byte = (x >> 48).toByte

  def byte5(x: Long): Byte = (x >> 40).toByte

  def byte4(x: Long): Byte = (x >> 32).toByte

  def byte3(x: Long): Byte = (x >> 24).toByte

  def byte2(x: Long): Byte = (x >> 16).toByte

  def byte1(x: Long): Byte = (x >> 8).toByte

  def byte0(x: Long): Byte = (x >> 0).toByte

  def byte3(x: Int): Byte = (x >> 24).toByte

  def byte2(x: Int): Byte = (x >> 16).toByte

  def byte1(x: Int): Byte = (x >> 8).toByte

  def byte0(x: Int): Byte = (x >> 0).toByte

  @inline
  def toUnsigned(value: Long): Long = value - Long.MinValue

  @inline
  def unsignedCompare(a: Long, b: Long): Int = {
    val va = toUnsigned(a)
    val vb = toUnsigned(b)
    if (va < vb) -1
    else if (va > vb) +1
    else 0
  }

  def zero(value: Long, mask: Long) = (value & mask) == 0

  def log2(value: Long) = java.lang.Long.bitCount(java.lang.Long.highestOneBit(value))
}
