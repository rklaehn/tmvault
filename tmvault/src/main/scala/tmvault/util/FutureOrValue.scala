package tmvault.util

import scala.concurrent.{ExecutionContext, Future}

/**
 * Like a future, except that it has less (no) overhead in case the value is already known at creation.
 * Basically a future where
 * - Future.successful(x) is a NOOP instead of creating a bunch of objects
 * - combining two values that are already known has little overhead
 */
class FutureOrValue[T](private val wrapped: AnyRef) extends AnyVal {

  def isFuture = wrapped.isInstanceOf[Future[_]]

  def isValue = !isFuture

  private def value: T = wrapped.asInstanceOf[T]

  private def future: Future[T] = wrapped.asInstanceOf[Future[T]]
}

object FutureOrValue {

  def map2[T, U, V](a: FutureOrValue[T], b: FutureOrValue[U])(f: (T, U) => V)(implicit ec:ExecutionContext): FutureOrValue[V] = {
    if (a.isValue) {
      if (b.isValue)
        fromValue(f(a.value, b.value))
      else
        fromFuture(b.future.map(b => f(a.value, b)))
    } else {
      if (b.isValue)
        fromFuture(a.future.map(a => f(a, b.value)))
      else
        fromFuture(b.future.flatMap(b => a.future.map(a => f(a, b))))
    }
  }

  def fromValue[T](value: T) = new FutureOrValue[T](value.asInstanceOf[AnyRef])

  def fromFuture[T](future: Future[T]) = new FutureOrValue[T](future)
}
