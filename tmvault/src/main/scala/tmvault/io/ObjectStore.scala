package tmvault.io

import tmvault.{Future, ExecutionContext}
import tmvault.util.SHA1Hash

object ObjectStore {

  def apply[T](blockStore: BlockStore, serializer: BlobSerializer[T])(implicit ec: ExecutionContext): ObjectStore[T] =
    SimpleObjectStore(blockStore, serializer)

  private case class SimpleObjectStore[T](blockStore: BlockStore, serializer: BlobSerializer[T])(implicit ec: ExecutionContext) extends ObjectStore[T] {

    override def get(key: SHA1Hash): Future[T] =
      blockStore.getBytes(key).map(bytes => serializer.read(BlobIterator(bytes)))

    override def put(value: T): Future[SHA1Hash] = {
      val size = serializer.size(value)
      val builder = BlobBuilder(size)
      serializer.write(value, builder)
      blockStore.putBytes(builder.result)
    }
  }
}

trait ObjectStore[T] {

  def put(value:T) : Future[SHA1Hash]

  def get(key:SHA1Hash) : Future[T]
}