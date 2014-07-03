package tmvault.eager

import tmvault.util.SHA1Hash

import scala.concurrent.Future

/**
 * The frontend to a CAS storage backend
 */
trait BlockStore[T] {

  def get(hash:SHA1Hash) : Future[T]

  def put(value:T) : Future[SHA1Hash]
}
