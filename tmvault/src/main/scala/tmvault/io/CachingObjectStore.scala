package tmvault.io

import com.google.common.cache.CacheBuilder
import tmvault.Future
import tmvault.util.SHA1Hash
import tmvault.ExecutionContext.Implicits.global

object CachingObjectStore {

  def apply[T <: AnyRef](wrapped: ObjectStore[T]) : ObjectStore[T] =
    new CachingObjectStore[T](wrapped)

  class CachingObjectStore[T <: AnyRef](wrapped: ObjectStore[T]) extends ObjectStore[T] {

    val cache = CacheBuilder.newBuilder()
      .softValues()
      .maximumSize(1000)
      .build[SHA1Hash, T]()

    override def put(value: T): Future[SHA1Hash] = {
      wrapped.put(value).map { hash =>
        cache.put(hash, value)
        hash
      }
    }

    override def get(key: SHA1Hash): Future[T] = {
      Option(cache.getIfPresent(key)) match {
        case Some(x) =>
          Future.successful(x)
        case None =>
          wrapped.get(key).map { value =>
            cache.put(key, value)
            value
          }
      }
    }
  }

}