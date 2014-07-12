package tmvault.eager

import java.lang.Long.{bitCount, highestOneBit}

import tmvault.util.SHA1Hash
import tmvault.util.ArrayUtil._

import scala.util.hashing.MurmurHash3

/**
 * The concurrent but eager version of the index tree.
 */
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
   * This is used to determine whether to place an indirection above this node
   */
  def weight: Int

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

  def foreachLeaf[U](f: Leaf => U): Unit

  require(min == (min & mask))
}

final case class Branch(min: Long, level: Int, left: Node, right: Node) extends Node {
  require(min <= left.min && left.max <= center)
  require(center <= right.min && right.max <= max)

  def containsReferences = left.containsReferences || right.containsReferences

  val size = left.size + right.size

  def weight = left.weight + right.weight

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

  def weight = IndexTreeSerializer.DataSerializer.size(this) + 1 // Int.MaxValue //

  def split: (Node, Node) = {
    if (level == 0)
      throw new UnsupportedOperationException
    val pivot = min + width / 2
    val splitIndex = data.firstIndexWhereGE(pivot)
    val left = data.takeUnboxed(splitIndex)
    val right = data.dropUnboxed(splitIndex)
    (Data(left), Data(right))
  }

  def copyToArray(target: Array[Long], offset: Int): Unit = {
    System.arraycopy(data, 0, target, offset, data.length)
  }

  def size = data.length

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

object Data {
  def apply(data: Array[Long]): Data = {
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

final case class Reference(min: Long, level: Int, size: Long, hash: SHA1Hash) extends Leaf {

  def containsReferences = true

  def weight = IndexTreeSerializer.ReferenceSerializer.size(this) + 1

  def getChild(implicit c: IndexTreeContext) =
    c.store.get(hash)

  def split = throw new UnsupportedOperationException

  def copyToArray(target: Array[Long], offset: Int) = throw new UnsupportedOperationException

  override def foreachLeaf[U](f: Leaf => U): Unit = f(this)
}

