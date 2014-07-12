package tmvault.eager

import java.lang.Long.{bitCount, highestOneBit}

import tmvault.util.SHA1Hash
import tmvault.util.ArrayUtil._
import scala.util.hashing.MurmurHash3
import Node._

sealed abstract class Node {

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
   * Splits the tree. This is not possible if the tree is a level 0 leaf or reference
   */
  def split: (Node, Node)

  /**
   * Flag to check if a tree contains references into a data store
   */
  def containsReferences: Boolean

  /**
   * The number of elements in this tree. This is O(1) for all implementations
   */
  def size: Long

  /**
   * Copies the content of this tree to an array. This might not be possible if the tree contains references
   */
  def copyToArray(target: Array[Long], offset: Int): Unit

  /**
   * flattens this tree to an ordered sequence of leafs
   */
  final def leafs: Traversable[Leaf] = new Traversable[Leaf] {
    override def foreach[U](f: Leaf => U): Unit = foreachLeaf(f)
  }

  def bytes: Int

  def foreachLeaf[U](f: Leaf => U): Unit

  require(min == (min & mask))
}

object Node {

  /**
   * Returns true if a and b overlap, false if they are disjoint
   */
  def overlap(a: Node, b: Node): Boolean =
    (a.min & b.mask) == (b.min & a.mask)

  /**
   * Creates a new branch node above two non-overlapping trees of arbitrary order
   */
  def mkBranch(a: Node, b: Node): Branch = {
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

  def mkLeaf(data: Array[Long]): Data = {
    require(data.isIncreasing())
    require(data(0) >= 0L)
    if (data.length == 1)
      Data(data(0), 0, data)
    else {
      val a = data(0)
      val b = data(data.length - 1)
      val pivot = highestOneBit(a ^ b)
      val mask = pivot | (pivot - 1)
      val min = a & ~mask
      val level = bitCount(mask)
      Data(min, level, data)
    }
  }
}

final case class Branch(min: Long, level: Int, left: Node, right: Node) extends Node {
  require(min <= left.min && left.max <= center)
  require(center <= right.min && right.max <= max)

  val containsReferences = left.containsReferences || right.containsReferences

  val size = left.size + right.size

  val bytes = left.bytes + right.bytes

  def split = (left, right)

  override def copyToArray(target: Array[Long], offset: Int): Unit = {
    left.copyToArray(target, offset)
    right.copyToArray(target, offset + left.size.toInt)
  }

  override def toString: String = s"Branch($min, $level, $size)"

  override def foreachLeaf[U](f: Leaf => U): Unit = {
    left.foreachLeaf(f)
    right.foreachLeaf(f)
  }
}

sealed trait Leaf extends Node

final case class Data(min: Long, level: Int, data: Array[Long]) extends Leaf {

  def containsReferences = false

  def split: (Node, Node) = {
    if (level == 0)
      throw new UnsupportedOperationException
    val pivot = min + width / 2
    val splitIndex = data.firstIndexWhereGE(pivot)
    val left = data.takeUnboxed(splitIndex)
    val right = data.dropUnboxed(splitIndex)
    (mkLeaf(left), mkLeaf(right))
  }

  def copyToArray(target: Array[Long], offset: Int): Unit = {
    System.arraycopy(data, 0, target, offset, data.length)
  }

  def size = data.length

  def bytes = data.length * 8 + 4 + 1

  override def foreachLeaf[U](f: Leaf => U): Unit = f(this)

  override def hashCode(): Int =
    MurmurHash3.arrayHash(data)

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: Data => java.util.Arrays.equals(this.data, that.data)
    case _ => false
  }

  override def toString = {
    val dataText = data.mkString("[", ",", "]")
    val reducedText = if (dataText.length > 60) dataText.take(57) + "..." else dataText
    s"Leaf($min, $level, $size, $reducedText)"
  }
}

final case class Reference(min: Long, level: Int, size: Long, hash: SHA1Hash) extends Leaf {

  def containsReferences = true

  def split = throw new UnsupportedOperationException

  def copyToArray(target: Array[Long], offset: Int) = throw new UnsupportedOperationException

  def bytes = 20 + 8 + 8 + 4 + 1

  override def foreachLeaf[U](f: Leaf => U): Unit = f(this)
}

