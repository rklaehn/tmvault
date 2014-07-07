package tmvault.util

import java.nio.{ByteBuffer, ByteOrder}
import java.security.{MessageDigest, NoSuchAlgorithmException}

final class SHA1Hash private(private val part1: Long, private val part2: Long, private val part3: Int) {

  override def hashCode = part3

  override def equals(that: Any) = that match {
    case that: SHA1Hash => this.part1 == that.part1 && this.part2 == that.part2 && this.part3 == that.part3
    case _ => false
  }

  override def toString = SHA1Hash.toString(this)

  def ^(that: SHA1Hash) = new SHA1Hash(this.part1 ^ that.part1, this.part2 ^ that.part2, this.part3 ^ that.part3)

  def copyToArray(target:Array[Byte], offset:Int) : Unit = {
    var i = 0
    while(i<20) {
      target(offset + i) = apply(i)
      i+=1
    }
  }

  def apply(index: Int): Byte = {
    import tmvault.util.BitUtil._
    index match {
      case 0 => byte0(part1)
      case 1 => byte1(part1)
      case 2 => byte2(part1)
      case 3 => byte3(part1)
      case 4 => byte4(part1)
      case 5 => byte5(part1)
      case 6 => byte6(part1)
      case 7 => byte7(part1)

      case 8 => byte0(part2)
      case 9 => byte1(part2)
      case 10 => byte2(part2)
      case 11 => byte3(part2)
      case 12 => byte4(part2)
      case 13 => byte5(part2)
      case 14 => byte6(part2)
      case 15 => byte7(part2)

      case 16 => byte0(part3)
      case 17 => byte1(part3)
      case 18 => byte2(part3)
      case 19 => byte3(part3)

      case _ =>
        throw new IndexOutOfBoundsException
    }
  }

  def write(buffer: ByteBuffer): ByteBuffer = {
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.putLong(part1)
    buffer.putLong(part2)
    buffer.putInt(part3)
    buffer
  }

  def toArray : Array[Byte] = {
    val result = new Array[Byte](20)
    copyToArray(result, 0)
    result
  }
}

object SHA1Hash {

  val zero = new SHA1Hash(0,0,0)

  def hash(data: Array[Byte]) : SHA1Hash = {
    val digest = sha1Digest.get
    digest.reset()
    digest.update(data)
    apply(ByteBuffer.wrap(digest.digest))
  }

  def apply(buffer: ByteBuffer) = {
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val part1 = buffer.getLong
    val part2 = buffer.getLong
    val part3 = buffer.getInt
    new SHA1Hash(part1, part2, part3)
  }

  def parse(text: String): SHA1Hash = {
    if (text.length != 40) throw new IllegalArgumentException
    val bytes = new Array[Byte](20)
    var i = 0
    while (i < 20) {
      bytes(i) = Integer.parseInt(text.substring(i * 2, i * 2 + 2), 16).asInstanceOf[Byte]
      i += 1
    }
    SHA1Hash(ByteBuffer.wrap(bytes))
  }

  private val hexes = "0123456789ABCDEF".toCharArray

  private def toString(hash: SHA1Hash): String = {
    val result = new StringBuilder(40)
    var i = 0
    while (i < 20) {
      val value = hash(i)
      result.append(hexes((value >> 4) & 0xF))
      result.append(hexes(value & 0xF))
      i += 1
    }
    result.toString()
  }

  private final val sha1Digest: ThreadLocal[MessageDigest] = new ThreadLocal[MessageDigest] {
    protected override def initialValue: MessageDigest = {
      try {
        MessageDigest.getInstance("SHA-1")
      } catch {
        case e: NoSuchAlgorithmException => null
      }
    }
  }

}