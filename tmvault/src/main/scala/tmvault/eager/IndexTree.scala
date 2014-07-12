package tmvault.eager

import scala.concurrent.Future
import tmvault.util.ArrayUtil._
import java.lang.Long.{bitCount, highestOneBit}

object IndexTree {

  import scala.concurrent.ExecutionContext.Implicits.global

  /**
   * The right and left subtree of this node.
   * - In case of a leaf node, these have to be created on demand
   * - In case of a reference node, these have to be retrieved from a block store
   * therefore this method returns a future
   */
  def split(node: Node)(implicit c: IndexTreeContext): Future[(Node, Node)] = node match {
    case node: Data =>
      Future.successful(node.split)
    case node: Branch =>
      Future.successful(node.split)
    case node: Reference =>
      for {
        child <- node.getChild
        (l, r) <- split(child)
        l <- wrap(l)
        r <- wrap(r)
      } yield (l, r)
  }

  final def toArray(node: Node)(implicit c: IndexTreeContext): Future[Array[Long]] = {
    val target = new Array[Long](node.size.toInt)
    copyToArray(node, target, 0).map(_ => target)
  }

  /**
   * Converts all elements to an array. This will fail if the size of the tree is more than 2^32 elements.
   */
  def copyToArray(node: Node, target: Array[Long], offset: Int)(implicit c: IndexTreeContext): Future[Unit] = node match {
    case node: Data =>
      Future.successful(node.copyToArray(target, offset))
    case node: Reference =>
      node.getChild.flatMap(child => copyToArray(child, target, offset))
    case node: Branch =>
      for {
        _ <- copyToArray(node.left, target, offset)
        _ <- copyToArray(node.right, target, offset + node.left.size.toInt)
      } yield ()
  }

  def maxValues = 64

  def fromLong(value: Long): Future[Node] = Future.successful(mkLeaf(Array[Long](value)))

  def fromLongs(values: Array[Long])(implicit c: IndexTreeContext): Future[Node] = {
    require(values.length > 0)
    require(values.isIncreasing())
    def mkNode(from: Int, until: Int): Future[Node] = {
      if (until - from <= maxValues)
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

  def lines(tree: Node, indent: String = "  "): Traversable[String] = new Traversable[String] {
    override def foreach[U](f: (String) => U): Unit = show(tree, indent)(x => f(x))
  }

  def show(tree: Node, indent: String = "  ")(f: String => Unit = println): Unit = {
    def show0(tree: Node, prefix: String): Unit = tree match {
      case x: Leaf => f(prefix + x.toString)
      case b: Branch =>
        f(prefix + b.toString)
        show0(b.left, prefix + indent)
        show0(b.right, prefix + indent)
    }
    show0(tree, indent)
  }

  private[eager] def mergeNow(a: Node, b: Node): Node = {
    if (a.size + b.size <= maxValues && !a.containsReferences && !b.containsReferences) {
      val temp = new Array[Long]((a.size + b.size).toInt)
      a.copyToArray(temp, 0)
      b.copyToArray(temp, a.size.toInt)
      mkLeaf(temp.sortedAndDistinct)
    } else if (!overlap(a, b)) {
      // the two nodes do not overlap, so we can just create a branch node above them
      mkBranch(a, b)
    } else if (a.level > b.level) {
      // a is above b
      val (l, r) = a.split
      if (b.min < a.center)
        mkBranch(mergeNow(l, b), r)
      else
        mkBranch(l, mergeNow(r, b))
    } else if (a.level < b.level) {
      // b is above a
      val (l, r) = b.split
      if (a.min < b.center)
        mkBranch(mergeNow(a, l), r)
      else
        mkBranch(l, mergeNow(a, r))
    } else {
      // a and b have the same interval
      if (a.level == 0)
        a
      else {
        val (al, ar) = a.split
        val (bl, br) = b.split
        mkBranch(mergeNow(al, bl), mergeNow(ar, br))
      }
    }
  }

  def merge(a: Node, b: Node)(implicit c: IndexTreeContext): Future[Node] = {

    if (a.size + b.size <= maxValues) {
      val temp = new Array[Long]((a.size + b.size).toInt)
      for {
        _ <- copyToArray(a, temp, 0)
        _ <- copyToArray(b, temp, a.size.toInt)
        r <- wrap(mkLeaf(temp.sortedAndDistinct))
      } yield r
    } else if (!overlap(a, b)) {
      // the two nodes do not overlap, so we can just create a branch node above them
      for {
        a <- wrap(a)
        b <- wrap(b)
        r <- wrap(mkBranch(a, b))
      } yield r
    } else if (a.level > b.level) {
      // a is above b
      split(a).flatMap { case (l, r) =>
        if (b.min < a.center)
          merge(l, b).flatMap(l => wrap(mkBranch(l, r)))
        else
          merge(r, b).flatMap(r => wrap(mkBranch(l, r)))
      }
    } else if (a.level < b.level) {
      // b is above a
      split(b).flatMap { case (l, r) =>
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
          (al, ar) <- split(a)
          (bl, br) <- split(b)
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
  def overlap(a: Node, b: Node): Boolean =
    (a.min & b.mask) == (b.min & a.mask)

  def wrap(a: Node)(implicit c: IndexTreeContext): Future[Node] = {
    if (a.weight < c.maxWeight)
      Future.successful(a)
    else
      c.store.put(a).map { hash =>
        Reference(a.min, a.level, a.size, hash)
      }
  }

  /**
   * Creates a new branch node above two non-overlapping trees of arbitrary order
   */
  def mkBranch(a: Node, b: Node): Branch = {
    //    if(a.isInstanceOf[Data] && b.isInstanceOf[Data])
    //      require(!a.isInstanceOf[Data] || !b.isInstanceOf[Data])
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
