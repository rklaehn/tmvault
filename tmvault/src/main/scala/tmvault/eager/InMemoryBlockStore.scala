//package tmvault.eager
//
//import java.nio.{ByteOrder, ByteBuffer}
//
//import tmvault.eager.IndexTree.{Leaf, Branch}
//import tmvault.util.SHA1Hash
//
//import scala.concurrent.Future
//
//class InMemoryBlockStore extends BlockStore[IndexTree] {
//  @volatile
//  private var data:Map[SHA1Hash, IndexTree]
//
//  def toBytes(node: IndexTree) = node match {
//    case l: Leaf =>
//      val result = ByteBuffer.allocate(l.size.toInt * 8 + 2)
//      result.order(ByteOrder.LITTLE_ENDIAN)
//      result.put(0.toByte) // no compression
//      result.put(0.toByte) // index array
//    var i = 0
//      while (i < l.data.length) {
//        result.putLong(l.data(i))
//        i += 1
//      }
//      result.array()
//    case b: Branch =>
//      val references = b.references.toArray
//      val result = new Array[Byte](references.length * 20 + 2)
//      result(0) = 0.toByte // no compression
//      result(1) = 1.toByte // hash array
//    var i = 0
//      while (i < references.length) {
//        references(i).hash.copyToArray(result, i * 20 + 2)
//        i += 1
//      }
//      result
//    case _ =>
//      throw new UnsupportedOperationException
//  }
//
//  def fromBytes(bytes:Array[Byte]) : IndexTree = {
//    require(bytes.length>=2)
//    val compression = bytes(0)
//    val kind = bytes(1)
//    if(compression!=0)
//      throw new UnsupportedOperationException("compression not supported")
//    kind match {
//      case 0 =>
//        val buffer = ByteBuffer.wrap(bytes)
//        buffer.order(ByteOrder.LITTLE_ENDIAN)
//        buffer.position(2)
//        val size = (bytes.length - 2) / 8
//        val data = new Array[Long](size)
//        var i = 0
//        while(i<data.length) {
//          data(i) = buffer.getLong()
//          i+=1
//        }
//        IndexTree.mkLeaf(data)
//      case 1 =>
//        val size = (bytes.length - 2) / 20
//        val hashes = new Array[SHA1Hash](size)
//        ???
//      case _ =>
//        throw new UnsupportedOperationException("unknown  not supported")
//
//    }
//  }
//
//  override def get(hash: SHA1Hash): Future[IndexTree] = {
//
//  }
//
//  override def put(value: IndexTree): Future[SHA1Hash] = {
//
//  }
//}
