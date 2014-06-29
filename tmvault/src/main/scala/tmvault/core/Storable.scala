package tmvault.core

trait Storable[@specialized T] {

  def bits(value:T) : Int

  def store(value:T, target: BlockBuilder) : Unit

  def load(source: BlockBuilder) : T
}

object Storable {

  implicit object ByteIsStorable extends Storable[Byte] {

    def bits(value:Byte) = 8

    def store(value: Byte, target:BlockBuilder) : Unit = target.addByte(value)

    override def load(source: BlockBuilder): Byte = ???
  }
}