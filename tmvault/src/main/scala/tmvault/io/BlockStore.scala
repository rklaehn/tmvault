package tmvault.io

import tmvault.util.SHA1Hash
import tmvault.Future

import scala.concurrent.ExecutionContext

trait BlockStore {
  
  def putBytes(data: Array[Byte]): Future[SHA1Hash]

  def getBytes(key: SHA1Hash): Future[Array[Byte]]
}

object BlockStore {

  trait Writable[T] extends Any {

    def toByteArray(value: T) : Array[Byte]

    def fromByteArray(value: Array[Byte]) : T
  }

  object Implicits {

    implicit class WriteWritable(private val store: BlockStore) extends AnyVal {
      final def put[T](value: T)(implicit w: Writable[T]): Future[SHA1Hash] = {
        store.putBytes(w.toByteArray(value))
      }

      final def get[T](key: SHA1Hash)(implicit w: Writable[T], ec:ExecutionContext): Future[T] = {
        store.getBytes(key).map(w.fromByteArray)
      }
    }
  }
} 