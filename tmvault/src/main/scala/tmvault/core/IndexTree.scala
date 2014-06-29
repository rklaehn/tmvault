package tmvault.core

import debox._

/**
 * Typeclass for index tree context values such as config settings and memo table
 * @param maxValues the maximum number of allowed values (longs) in a leaf before it gets split
 */
final case class IndexTreeContext(maxValues: Int)

sealed abstract class IndexTree {

  def prefix: Long

  def level: Long

  def size: Long

  def leafCount: Long

  final def min: Long = prefix

  final def center = prefix + level

  final def max: Long = prefix + level * 2
  
  final def mask = level * 2 - 1

  final def merge(that:IndexTree)(implicit context:IndexTreeContext) : IndexTree =
    new Merge().apply(this, that)

  def toArray : Array[Long] = {
    require(size < Int.MaxValue)
    val result = new Array[Long](size.toInt)
    copyToArray(result, 0)
    result
  }

  def copyToArray(target:Array[Long], offset:Int) : Unit
}

class IndexTreeBuilder(implicit context:IndexTreeContext) {

  var buffer = Buffer.ofSize[Long](128)

  var tree = Option.empty[IndexTree]

  def ++=(values: Array[Long]) : Unit =
    if(!values.isEmpty) {
      require(buffer.isEmpty || buffer(buffer.length - 1) < values(0))
      buffer ++= values
      buildTree()
    }

  def +=(value: Long) : Unit = {
    require(buffer.isEmpty || buffer(buffer.length - 1) < value)
    buffer += value
    buildTree()
  }

  private def buildTree() : Unit =  {
    if (buffer.length > context.maxValues) {
      val chunks = IndexTree.chunk(buffer.elems, 0, buffer.length, context.maxValues)
      require(chunks.length > 1)
      buffer = Buffer.unsafe(chunks.last)
      var i = 0
      while(i< chunks.length -1) {
        tree = insert(tree, chunks(i))
        i+=1
      }
    }
  }

  private def insert(current:Option[IndexTree], chunk:Array[Long]) : Option[IndexTree] = {
    val leaf = IndexTree.mkLeaf(chunk)
    Some(current.map(_.merge(leaf)).getOrElse(leaf))
  }
}

object IndexTree {

  import java.lang.Long.highestOneBit
  import ArrayUtil._

  def debugPrint(tree: IndexTree): Unit = {
    def print0(tree: IndexTree, indent: String) : Unit = tree match {
      case Leaf(prefix, level, data) =>
        println(s"${indent}Leaf($prefix, $level)")
        println(indent + "  " +data.mkString("[",",","]"))
      case Branch(prefix, level, size, left, right) =>
        println(s"${indent}Branch($prefix,$level,$size)")
        print0(left, indent + "  ")
        print0(right, indent + "  ")
    }
    print0(tree, "")
  }

  case class Leaf(prefix: Long, level: Long, data: Array[Long]) extends IndexTree {

    def size = data.length

    def leafCount = 1L

    def split : (Leaf, Leaf) = {
      val index = firstIndexWhereGE(data, 0, data.length, center)
      val left = new Array[Long](index)
      val right = new Array[Long](data.length - index)
      System.arraycopy(data, 0, left, 0, left.length)
      System.arraycopy(data, left.size, right, 0, right.length)
      (mkLeaf(left), mkLeaf(right))
    }

    override def copyToArray(target: Array[Long], offset: Int): Unit = {
      System.arraycopy(data, 0, target, offset, data.length)
    }
  }

  case class Branch(prefix: Long, level: Long, size: Long, left: IndexTree, right: IndexTree) extends IndexTree {

    override def copyToArray(target: Array[Long], offset: Int): Unit = {
      if(offset + size > target.length)
        throw new IndexOutOfBoundsException()
      left.copyToArray(target, offset)
      right.copyToArray(target, offset + left.size.toInt)
    }

    def leafCount = left.leafCount + right.leafCount
  }

  /*
  case class Reference(prefix: Long, level: Long, size:Long, hash: SHA1Hash) extends IndexTree {

    override def mergeOverlapping(that: IndexTree)(implicit context: IndexTreeContext): IndexTree = ???
  }
  */

  final def disjoint(a:IndexTree, b:IndexTree) : Boolean = {
    val keep = ~(a.mask | b.mask)
    val p0 = a.prefix & keep
    val p1 = b.prefix & keep
    p0 != p1
  }

  def chunk(data: Array[Long], from:Int, until:Int, minSize: Int): Array[Array[Long]] = {
    require(minSize >= 1)
    require(isIncreasing(data, from, until))
    val builder = Array.newBuilder[Array[Long]]
    def chunk0(from: Int, until: Int): Unit =
      if (until - from <= minSize)
        builder += data.slice(from, until)
      else {
        val pivot = center(data(from), data(until - 1))
        val splitIndex = firstIndexWhereGE(data, from, until, pivot)
        chunk0(from, splitIndex)
        chunk0(splitIndex, until)
      }
    chunk0(from, until)
    builder.result()
  }

  def mkLeaf(a:IndexTree, b:IndexTree) : Leaf = {
    val size = a.size + b.size
    require(size < Int.MaxValue)
    if(disjoint(a,b)) {
      val data = new Array[Long](size.toInt)
      a.copyToArray(data, 0)
      b.copyToArray(data, a.size.toInt)
      java.util.Arrays.sort(data)
      mkLeaf(data)
    } else {
      val data = new Array[Long](size.toInt)
      a.copyToArray(data, 0)
      b.copyToArray(data, a.size.toInt)
      java.util.Arrays.sort(data)
      val remaining = removeDuplicates(data, 0, data.length)
      if(remaining != data.length) {
        val temp = new Array[Long](remaining)
        System.arraycopy(data, 0, temp, 0, remaining)
        mkLeaf(temp)
      } else
        mkLeaf(data)
    }
  }

  def mkLeaf(data: Array[Long]): Leaf = {
    assert(isIncreasing(data, 0, data.length))
    assert(data.length > 0)
    val min = data(0)
    val max = data(data.length - 1)
    val level = branchLevel(min, max)
    val prefix = min & ~(level * 2 - 1)
    Leaf(prefix, level, data)
  }

  def mkBranch(a: IndexTree, b: IndexTree): IndexTree = {
    val mask1 = branchLevel(a.prefix, b.prefix)
    val prefix1 = mask(a.prefix, mask1)
    val size1 = a.size + b.size
    if (zero(a.prefix, mask1))
      Branch(prefix1, mask1, size1, a, b)
    else
      Branch(prefix1, mask1, size1, b, a)
  }

  private def center(min: Long, max: Long): Long = {
    val bit = branchLevel(min, max)
    val prefix = mask(min, bit)
    prefix | bit
  }

  private def toUnsigned(value: Long): Long = value - Long.MinValue

  private def unsignedCompare(a: Long, b: Long): Int = {
    val va = toUnsigned(a)
    val vb = toUnsigned(b)
    if (va < vb) -1
    else if (va > vb) +1
    else 0
  }

  private def hasMatch(key: Long, prefix: Long, m: Long) = mask(key, m) == prefix

  private def unsignedLT(i: Long, j: Long) = (i < j) ^ (i < 0L) ^ (j < 0L)

  private def higher(m1: Long, m2: Long) = unsignedLT(m2, m1)

  private def zero(value: Long, mask: Long) = (value & mask) == 0

  private def mask(value: Long, bit: Long): Long = value & ~(bit | (bit - 1))

  private def branchLevel(i: Long, j: Long): Long = highestOneBit(i ^ j)
}
