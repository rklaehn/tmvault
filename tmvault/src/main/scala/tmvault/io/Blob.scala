package tmvault.io

trait Blob {

  def size:Int

  def iterator: BlobIterator
}

object Blob {

  def apply(bytes:Array[Byte]) : Blob = new Impl(bytes.clone())

  private class Impl(private val wrapped:Array[Byte]) extends Blob {

    def size:Int = wrapped.length

    def iterator = BlobIterator(wrapped)
  }
}
