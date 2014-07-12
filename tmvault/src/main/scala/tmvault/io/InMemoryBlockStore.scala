package tmvault.io

import tmvault.util.SHA1Hash
import tmvault.{Future, ExecutionContext}

/**
 * A simple in-memory block store for testing
 */
object InMemoryBlockStore {

  def create(implicit ec: ExecutionContext): BlockStore = new InMemoryBlockStore()

  private object DataBlock {
    def apply(data: Array[Byte]) = {
      val cloned = data.clone()
      val key = SHA1Hash.hash(cloned)
      new DataBlock(key, cloned)
    }
  }

  private final class DataBlock(val key: SHA1Hash, data: Array[Byte]) {

    def toArray = data.clone()

    override def hashCode(): Int =
    // return the hash code of the SHA1 hash, which obviously has a very good distribution
      key.hashCode

    override def equals(obj: scala.Any): Boolean = obj match {
      case that: DataBlock =>
        // we consider two data blocks equal if their hash is equal. SHA1 is a reasonably strong hash after all
        this.key == that.key
      case _ => false
    }
  }

  private final class InMemoryBlockStore(implicit ec: ExecutionContext) extends BlockStore {

    private val store = new scala.collection.concurrent.TrieMap[SHA1Hash, DataBlock]()

    override def putBytes(data: Array[Byte]): Future[SHA1Hash] =
      Future {
        val block = DataBlock(data)
        store.putIfAbsent(block.key, block)
        block.key
      }

    override def getBytes(key: SHA1Hash): Future[Array[Byte]] = {
      Future {
        store.apply(key).toArray
      }
    }

    override def toString = getClass.getSimpleName + "(" + store.size + ")"
  }

}