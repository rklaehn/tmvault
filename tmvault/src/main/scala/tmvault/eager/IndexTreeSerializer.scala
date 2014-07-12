package tmvault.eager

import tmvault.eager.Node._
import tmvault.io.{BlobIterator, BlobBuilder, BlobSerializer}
import BlobSerializer.PrimitiveSerializer._
import tmvault.util.SHA1Hash

case object IndexTreeSerializer extends BlobSerializer[Node] {

  implicit object SHA1HashSerializer extends BlobSerializer[SHA1Hash] {

    override def size(value: SHA1Hash): Int = 20

    override def write(value: SHA1Hash, target: BlobBuilder): Unit = value.write(target)

    override def read(source: BlobIterator): SHA1Hash = SHA1Hash(source)
  }

  implicit object DataSerializer extends BlobSerializer[Data] {

    override def size(value: Data): Int = 4 + value.data.length * 8

    override def write(value: Data, target: BlobBuilder): Unit = {
      target.putInt(value.data.length)
      target.putLongs(value.data)
    }

    override def read(source: BlobIterator): Data = {
      val length = source.getInt()
      val data = source.getLongs(length)
      mkLeaf(data)
    }
  }

  implicit object ReferenceSerializer extends BlobSerializer[Reference] {

    override def size(value: Reference): Int =
      8 + 8 + 4 + 20

    override def write(value: Reference, target: BlobBuilder): Unit = {
      target.put[Long](value.size)
      target.put[Long](value.min)
      target.put[Int](value.level)
      target.put[SHA1Hash](value.hash)
    }

    override def read(source: BlobIterator): Reference = {
      val size = source.get[Long]
      val min = source.get[Long]
      val level = source.get[Int]
      val hash = source.get[SHA1Hash]
      Reference(min, level, size, hash)
    }
  }

  object LeafSerializer extends BlobSerializer[Leaf] {

    override def size(value: Leaf): Int = value match {
      case leaf: Reference =>
        1 + ReferenceSerializer.size(leaf)
      case leaf: Data =>
        1 + DataSerializer.size(leaf)
    }

    override def write(value: Leaf, target: BlobBuilder): Unit = value match {
      case leaf: Reference =>
        target.put(0.toByte)
        ReferenceSerializer.write(leaf, target)
      case leaf: Data =>
        target.put(1.toByte)
        DataSerializer.write(leaf, target)
    }

    override def read(source: BlobIterator): Leaf = {
      source.getByte() match {
        case 0 =>
          ReferenceSerializer.read(source)
        case 1 =>
          DataSerializer.read(source)
        case _ =>
          ???
      }
    }
  }

  val leafSeqSerializer = BlobSerializer.sequenceSerializer(LeafSerializer)

  override def size(value: Node): Int = leafSeqSerializer.size(value.leafs)

  override def write(value: Node, target: BlobBuilder): Unit = {
    val leafs = value.leafs.toArray
    //      val debug = leafs.map {
    //        case x:Data => "D"
    //        case x:Reference => "R"
    //      }.mkString("")
    //      if(debug.contains("RDR"))
    //        println(debug)
    leafSeqSerializer.write(value.leafs, target)
  }

  override def read(source: BlobIterator): Node = {
    val leafs = leafSeqSerializer.read(source)
    leafs.reduceLeft(mergeNow)
  }
}