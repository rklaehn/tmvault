package tmvault.core

final class SHA1Hash private(private val part1: Long, private val part2: Long, private val part3: Int) {

  override def hashCode = part3

  override def equals(that: Any) = that match {
    case that: SHA1Hash => this.part1 == that.part1 && this.part2 == that.part2 && this.part3 == that.part3
    case _ => false
  }

  override def toString = ???

  def apply(index: Int): Byte = {
    import BitUtil._
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

  def toByteArray = {
    val result = new Array[Byte](20)
    copyTo(result, 0)
    result
  }

  def copyTo(target:Array[Byte], offset:Int) : Unit =
    ???
}

object SHA1Hash {

  def parse(text: String): SHA1Hash = ???
}