package object tmvault {

/*
  type Future[T] = scala.concurrent.Future[T]

  def Future = scala.concurrent.Future

  def Await = scala.concurrent.Await

  type ExecutionContext = scala.concurrent.ExecutionContext

  val ExecutionContext = scala.concurrent.ExecutionContext

*/
  type Future[T] = tmvault.util.Future[T]

  def Future = tmvault.util.Future

  def Await = tmvault.util.Await

  type ExecutionContext = tmvault.util.ExecutionContext

  val ExecutionContext = tmvault.util.ExecutionContext
}
