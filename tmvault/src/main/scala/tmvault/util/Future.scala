package tmvault.util

class Future[+T](private[util] val value:T) {

  def withFilter(p:T=>Boolean) : Future[T] = this

  def filter(p:T=>Boolean) : Future[T] = this

  def map[U](f:T=>U) : Future[U] = Future(f(value))

  def flatMap[U](f:T=>Future[U]) : Future[U] = f(value)
}

object Future {
  def successful[T](value:T) : Future[T] = new Future(value)

  def apply[T](f: => T) : Future[T] = new Future(f)
}

object Await {

  def result[T](f:Future[T],d:scala.concurrent.duration.FiniteDuration) : T = f.value
}

trait ExecutionContext

object ExecutionContext {

  private val _global = new ExecutionContext {}

  def global = _global

  object Implicits {
    implicit def global = _global
  }
}