package tmvault.io

import java.io.File

import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._

import tmvault.Future
import tmvault.util.SHA1Hash
import scala.concurrent.ExecutionContext.Implicits.global

class LevelDBBlockStore(file:File) extends BlockStore with AutoCloseable {

  val options = new Options()
    .createIfMissing(true)
    .compressionType(CompressionType.NONE)

  val db = factory.open(file, options)

  override def putBytes(data: Array[Byte]): Future[SHA1Hash] = {
    Future {
      val hash = SHA1Hash.hash(data)
      db.put(hash.toArray, data)
      hash
    }
  }

  override def getBytes(key: SHA1Hash): Future[Array[Byte]] = {
    Future {
      db.get(key.toArray)
    }
  }

  override def close(): Unit = db.close()
}

object LevelDBBlockStore {
  def create(file:File) : BlockStore = new LevelDBBlockStore(file)
}