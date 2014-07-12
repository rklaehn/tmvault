package tmvault.io

trait BlobSerializer[@specialized T] {

  def size(value: T): Int

  def write(value: T, target: BlobBuilder): Unit

  def read(source: BlobIterator): T
}

object BlobSerializer {

  object PrimitiveSerializer {

    implicit val ofBoolean: BlobSerializer[Boolean] = new BlobSerializer[Boolean] {

      override def size(value: Boolean): Int = 1

      override def write(value: Boolean, target: BlobBuilder): Unit = target.putBoolean(value)

      override def read(source: BlobIterator): Boolean = source.getBoolean()
    }

    implicit val ofByte: BlobSerializer[Byte] = new BlobSerializer[Byte] {

      override def size(value: Byte): Int = 1

      override def write(value: Byte, target: BlobBuilder): Unit = target.putByte(value)

      override def read(source: BlobIterator): Byte = source.getByte()
    }

    implicit val ofShort: BlobSerializer[Short] = new BlobSerializer[Short] {

      override def size(value: Short): Int = 2

      override def write(value: Short, target: BlobBuilder): Unit = target.putShort(value)

      override def read(source: BlobIterator): Short = source.getShort()
    }

    implicit val ofInt: BlobSerializer[Int] = new BlobSerializer[Int] {

      override def size(value: Int): Int = 4

      override def write(value: Int, target: BlobBuilder): Unit = target.putInt(value)

      override def read(source: BlobIterator): Int = source.getInt()
    }

    implicit val ofLong: BlobSerializer[Long] = new BlobSerializer[Long] {

      override def size(value: Long): Int = 8

      override def write(value: Long, target: BlobBuilder): Unit = target.putLong(value)

      override def read(source: BlobIterator): Long = source.getLong()
    }

    implicit val ofFloat: BlobSerializer[Float] = new BlobSerializer[Float] {

      override def size(value: Float): Int = 4

      override def write(value: Float, target: BlobBuilder): Unit = target.putFloat(value)

      override def read(source: BlobIterator): Float = source.getFloat()
    }

    implicit val ofDouble: BlobSerializer[Double] = new BlobSerializer[Double] {

      override def size(value: Double): Int = 8

      override def write(value: Double, target: BlobBuilder): Unit = target.putDouble(value)

      override def read(source: BlobIterator): Double = source.getDouble()
    }
  }

  def sequenceSerializer[T](value: BlobSerializer[T]): BlobSerializer[Traversable[T]] = new SequenceSerializer[T](value)

  private class SequenceSerializer[T](wrapped: BlobSerializer[T]) extends BlobSerializer[Traversable[T]] {
    override def size(value: Traversable[T]): Int = value.foldLeft(0) { case (sum, elem) => sum + wrapped.size(elem)}

    override def write(value: Traversable[T], target: BlobBuilder): Unit = {
      value.foreach(elem => wrapped.write(elem, target))
    }

    override def read(source: BlobIterator): Traversable[T] = {
      val builder = IndexedSeq.newBuilder[T]
      while (source.hasRemaining) {
        builder += wrapped.read(source)
      }
      builder.result()
    }
  }

}
