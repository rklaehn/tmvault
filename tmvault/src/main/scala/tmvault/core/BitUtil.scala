package tmvault.core

private[core] object BitUtil {

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

}
