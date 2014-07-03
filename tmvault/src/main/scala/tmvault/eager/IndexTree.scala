package tmvault.eager

import java.lang.Long.{bitCount, highestOneBit}

import tmvault.eager.IndexTree.Reference
import tmvault.util.{SHA1Hash, ArrayUtil}

import scala.concurrent.Future
import scala.util.hashing.MurmurHash3

/**
 * The concurrent but eager version of the index tree.
 */
sealed abstract class IndexTree {

  /**
   * The minimum value (inclusive) of the interval of this node
   */
  def min: Long

  /**
   * The maximum value (exclusive) of the interval of this node
   */
  final def max = min + width

  /**
   * The level of the node. Higher levels mean larger intervals. A value of 0 means a node with only one possible value,
   * which should be extremely rare but must nevertheless be possible
   */
  def level: Int

  /**
   * The width of the node
   */
  final def width = 1L << level

  /**
   * The center of the node
   */
  final def center = min + width / 2

  /**
   * The bits of the min value that are the prefix
   */
  final def mask = -1L << level

  /**
   * The right and left subtree of this node.
   * - In case of a leaf node, these have to be created on demand
   * - In case of a reference node, these have to be retrieved from a block store
   * therefore this method returns a future
   */
  def split(implicit c: IndexTreeContext): Future[(IndexTree, IndexTree)]

  /**
   * The number of elements in this tree. This is O(1) for all implementations
   */
  def size: Long

  /**
   * This is used to determine whether to place an indirection above this node
   */
  def weight: Int

  /**
   * Converts all elements to an array. This will fail if the size of the tree is more than 2^32 elements.
   */
  def copyToArray(target: Array[Long], offset: Int)(implicit c: IndexTreeContext): Future[Unit]

  final def toArray(implicit c: IndexTreeContext) = {
    import c.executionContext
    val target = new Array[Long](size.toInt)
    copyToArray(target, 0).map(_ => target)
  }

  final def references : Traversable[Reference] = new Traversable[Reference] {
    override def foreach[U](f: (Reference) => U): Unit = foreachReference(f)
  }

  protected def foreachReference[U](f:Reference => U) : Unit

  require(min == (min & mask))
}

object IndexTree {

  import tmvault.util.ArrayUtil._

  def fromLong(value: Long): Future[IndexTree] = Future.successful(mkLeaf(Array[Long](value)))

  def fromLongs(values: Array[Long])(implicit c: IndexTreeContext): Future[IndexTree] = {
    require(values.length > 0)
    require(values.isIncreasing())
    import c.executionContext
    def mkNode(from: Int, until: Int): Future[IndexTree] = {
      if (until - from <= c.maxValues)
        Future.successful(mkLeaf(values.slice(from, until)))
      else {
        val a = values(from)
        val b = values(until - 1)
        val pivot = highestOneBit(a ^ b)
        val mask = pivot - 1
        val center = b & ~mask
        val splitIndex = values.firstIndexWhereGE(center, from, until)
        for {
          left <- mkNode(from, splitIndex)
          right <- mkNode(splitIndex, until)
          merged <- merge(left, right)
        } yield merged
      }
    }
    mkNode(0, values.length)
  }

  def lines(tree: IndexTree, indent: String = "  "): Traversable[String] = new Traversable[String] {
    override def foreach[U](f: (String) => U): Unit = show(tree, indent)(x => f(x))
  }

  def show(tree: IndexTree, indent: String = "  ")(f: String => Unit = println): Unit = {
    def show0(tree: IndexTree, prefix: String): Unit = tree match {
      case x: Leaf => f(prefix + x.toString)
      case b: Branch =>
        f(prefix + b.toString)
        show0(b.left, prefix + indent)
        show0(b.right, prefix + indent)
    }
    show0(tree, indent)
  }

  def merge(a: IndexTree, b: IndexTree)(implicit c: IndexTreeContext): Future[IndexTree] = {
    import c.executionContext
    if (a.size + b.size <= c.maxValues) {
      val temp = new Array[Long]((a.size + b.size).toInt)
      for {
        _ <- a.copyToArray(temp, 0)
        _ <- b.copyToArray(temp, a.size.toInt)
        r <- wrap(mkLeaf(temp.sortedAndDistinct))
      } yield r
    } else if (!overlap(a, b)) {
      // the two nodes do not overlap, so we can just create a branch node above them
      wrap(mkBranch(a, b))
    } else if (a.level > b.level) {
      // a is above b
      a.split.flatMap { case (l, r) =>
        if (b.min < a.center)
          merge(l, b).flatMap(l => wrap(mkBranch(l, r)))
        else
          merge(r, b).flatMap(r => wrap(mkBranch(l, r)))
      }
    } else if (a.level < b.level) {
      // b is above a
      b.split.flatMap { case (l, r) =>
        if (a.min < b.center)
          merge(a, l).flatMap(l => wrap(mkBranch(l, r)))
        else
          merge(a, r).flatMap(r => wrap(mkBranch(l, r)))
      }
    } else {
      // a and b have the same interval
      if (a.level == 0) {
        // if we get here, a and b contain the same value
        Future.successful(a)
      } else {
        for {
          (al, ar) <- a.split
          (bl, br) <- b.split
          l <- merge(al, bl)
          r <- merge(br, bl)
          r <- wrap(mkBranch(l, r))
        } yield r
      }
    }
  }

  /**
   * Returns true if a and b overlap, false if they are disjoint
   */
  def overlap(a: IndexTree, b: IndexTree): Boolean =
    (a.min & b.mask) == (b.min & a.mask)

  def wrap(a: IndexTree)(implicit c: IndexTreeContext): Future[IndexTree] = {
    import c.executionContext
    if (a.weight < c.maxWeight)
      Future.successful(a)
    else
      c.blockStore.put(a).map(hash => Reference(a.min, a.level, a.size, hash))
  }

  /**
   * Creates a new branch node above two non-overlapping trees of arbitrary order
   */
  def mkBranch(a: IndexTree, b: IndexTree): Branch = {
    require(!overlap(a, b))
    val pivot = highestOneBit(a.min ^ b.min)
    val mask = pivot | (pivot - 1)
    val min = a.min & ~mask
    val level = bitCount(mask)
    if (a.min < b.min)
      Branch(min, level, a, b)
    else
      Branch(min, level, b, a)
  }

  /**
   * Creates a leaf from the given data
   */
  def mkLeaf(data: Array[Long]): Leaf = {
    require(data.isIncreasing())
    require(data(0) >= 0L)
    if (data.length == 1)
      Leaf(data(0), 0, data)
    else {
      val a = data(0)
      val b = data(data.length - 1)
      val pivot = highestOneBit(a ^ b)
      val mask = pivot | (pivot - 1)
      val min = a & ~mask
      val level = bitCount(mask)
      Leaf(min, level, data)
    }
  }

  final case class Leaf(min: Long, level: Int, data: Array[Long]) extends IndexTree {

    def weight = Integer.MAX_VALUE

    def split(implicit c: IndexTreeContext): Future[(IndexTree, IndexTree)] = {
      if (level == 0)
        throw new UnsupportedOperationException
      val pivot = min + width / 2
      val splitIndex = data.firstIndexWhereGE(pivot)
      val left = data.takeUnboxed(splitIndex)
      val right = data.dropUnboxed(splitIndex)
      Future.successful((mkLeaf(left), mkLeaf(right)))
    }

    def size = data.length

    override def copyToArray(target: Array[Long], offset: Int)(implicit c: IndexTreeContext): Future[Unit] = {
      System.arraycopy(data, 0, target, offset, data.length)
      Future.successful(())
    }

    override protected def foreachReference[U](f: (Reference) => U): Unit = {
    }

    override def hashCode(): Int =
      MurmurHash3.arrayHash(data)

    override def equals(obj: scala.Any): Boolean = obj match {
      case that: Leaf => java.util.Arrays.equals(this.data, that.data)
      case _ => false
    }

    override def toString = {
      val dataText = data.mkString("[", ",", "]")
      val reducedText = if (dataText.length > 60) dataText.take(57) + "..." else dataText
      s"Leaf($min, $level, $size, $reducedText)"
    }
  }

  final case class Branch(min: Long, level: Int, left: IndexTree, right: IndexTree) extends IndexTree {
    require(min <= left.min && left.max <= center)
    require(center <= right.min && right.max <= max)

    val size = left.size + right.size

    def weight = left.weight + right.weight

    def split(implicit c: IndexTreeContext) = Future.successful((left, right))

    override def copyToArray(target: Array[Long], offset: Int)(implicit c: IndexTreeContext): Future[Unit] = {
      import c.executionContext
      for {
        _ <- left.copyToArray(target, offset)
        _ <- right.copyToArray(target, offset + left.size.toInt)
      } yield ()
    }

    override def toString: String = s"Branch($min, $level, $size)"

    override protected def foreachReference[U](f: (Reference) => U): Unit = {
      left.foreachReference(f)
      right.foreachReference(f)
    }
  }

  final case class Reference(min: Long, level: Int, size: Long, hash: SHA1Hash) extends IndexTree {

    def weight = 20

    def getChild(implicit c: IndexTreeContext) = c.blockStore.get(hash)

    def split(implicit c: IndexTreeContext) = {
      import c.executionContext
      for {
        child <- getChild
        lr <- child.split
      } yield lr
    }

    override def copyToArray(target: Array[Long], offset: Int)(implicit c: IndexTreeContext): Future[Unit] = {
      import c.executionContext
      for (child <- getChild)
      yield child.copyToArray(target, offset)
    }

    override protected def foreachReference[U](f: (Reference) => U): Unit = f(this)
  }

}