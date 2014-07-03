package tmvault.reference

import java.lang.Long.{highestOneBit, bitCount}

import tmvault.util.ArrayUtil

import scala.util.hashing.MurmurHash3

/**
 * The reference version of the index tree.
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
   * The right and left subtree of this node. In case of a leaf node, these have to be created on demand.
   */
  def split: (IndexTree, IndexTree)

  /**
   * The number of elements in this tree. This is O(1) for all implementations
   */
  def size:Long

  /**
   * Converts all elements to an array. This will fail if the size of the tree is more than 2^32 elements.
   */
  def toArray: Array[Long]

  require(min == (min & mask))
}

object IndexTree {
  import ArrayUtil._

  val maxSize = 32

  def fromLong(value:Long) : IndexTree = mkLeaf(Array[Long](value))

  def fromLongs(values:Array[Long]) : IndexTree = {
    require(values.length > 0)
    require(values.isIncreasing())
    def mkNode(from:Int, until:Int) : IndexTree = {
      if(until - from <= maxSize)
        mkLeaf(values.slice(from, until))
      else {
        val a = values(from)
        val b = values(until - 1)
        val pivot = highestOneBit(a ^ b)
        val mask = pivot - 1
        val center = b & ~mask
        val splitIndex = values.firstIndexWhereGE(center, from, until)
        val left = mkNode(from, splitIndex)
        val right = mkNode(splitIndex, until)
        merge(left, right)
      }
    }
    mkNode(0, values.length)
  }

  def lines(tree:IndexTree, indent:String = "  ") : Traversable[String] = new Traversable[String] {
    override def foreach[U](f: (String) => U): Unit = show(tree, indent)(x => f(x))
  }

  def show(tree:IndexTree, indent:String = "  ")(f:String => Unit = println) : Unit = {
    def show0(tree:IndexTree, prefix:String) : Unit = tree match {
      case x:Leaf => f(prefix + x.toString)
      case b:Branch =>
        f(prefix + b.toString)
        show0(b.left, prefix + indent)
        show0(b.right, prefix + indent)
    }
    show0(tree, indent)
  }

  def merge(a: IndexTree, b: IndexTree): IndexTree = {
    if(a.size + b.size <= maxSize) {
      val leafData =
        if(!overlap(a,b)) {
          if (a.min < b.min)
            a.toArray concat b.toArray
          else
            b.toArray concat a.toArray
        } else {
          (a.toArray concat b.toArray).sortedAndDistinct
        }
      mkLeaf(leafData)
    }
    else if (!overlap(a, b)) {
      // the two nodes do not overlap, so we can just create a branch node above them
      mkBranch(a, b)
    } else if (a.level > b.level) {
      // a is above b
      val (l,r) = a.split
      if (b.min < a.center)
        mkBranch(merge(l, b), r)
      else
        mkBranch(l, merge(r, b))
    } else if (a.level < b.level) {
      // b is above a
      val (l,r) = b.split
      if (a.min < b.center)
        mkBranch(merge(a, l), r)
      else
        mkBranch(l, merge(a, r))
    } else {
      // a and b have the same interval
      if(a.level == 0)
        a
      else {
        val (al, ar) = a.split
        val (bl, br) = b.split
        mkBranch(merge(al, bl), merge(ar, br))
      }
    }
  }

  /**
   * Returns true if a and b overlap, false if they are disjoint
   */
  def overlap(a: IndexTree, b: IndexTree): Boolean =
    (a.min & b.mask) == (b.min & a.mask)

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

    def split = {
      if(level == 0)
        throw new UnsupportedOperationException
      val pivot = min + width / 2
      val splitIndex = data.firstIndexWhereGE(pivot)
      val left = data.takeUnboxed(splitIndex)
      val right = data.dropUnboxed(splitIndex)
      (mkLeaf(left), mkLeaf(right))
    }

    def size = data.length

    def toArray = data

    override def hashCode(): Int =
      MurmurHash3.arrayHash(data)

    override def equals(obj: scala.Any): Boolean = obj match {
      case that:Leaf => java.util.Arrays.equals(this.data, that.data)
      case _ => false
    }

    override def toString = {
      val dataText = data.mkString("[",",","]")
      val reducedText = if(dataText.length > 60) dataText.take(57) + "..." else dataText
      s"Leaf($min, $level, $size, $reducedText)"
    }
  }

  final case class Branch(min: Long, level: Int, left: IndexTree, right: IndexTree) extends IndexTree {
    require(min <= left.min && left.max <= center)
    require(center <= right.min && right.max <= max)

    val size = left.size + right.size

    def split = (left, right)

    def toArray = left.toArray ++ right.toArray

    override def toString: String = s"Branch($min, $level, $size)"
  }

}