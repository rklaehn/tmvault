package tmvault.eager

import tmvault.io.{BlockStore, BlobSerializer, ObjectStore}

import scala.concurrent.{ExecutionContext, Future}
import tmvault.util.ArrayUtil._
import java.lang.Long.{bitCount, highestOneBit}
import Node._

object IndexTree {
  def create(blockStore: BlockStore, maxValues:Int = 32, maxWeight:Int = 32768)(implicit ec:ExecutionContext) : IndexTree =
    new SimpleIndexTree(maxValues, maxWeight, blockStore, ec)

  private case class SimpleIndexTree(maxValues:Int, maxWeight:Int, blockStore: BlockStore, executionContext:ExecutionContext) extends IndexTree {

    override val serializer: BlobSerializer[Node] = IndexTreeSerializer(this)

    override val objectStore: ObjectStore[Node] = ObjectStore(blockStore, serializer)(executionContext)
  }
}

abstract class IndexTree {

  def maxValues : Int

  def maxWeight : Int

  def serializer: BlobSerializer[Node]

  def objectStore: ObjectStore[Node]

  implicit def executionContext : ExecutionContext

  def getChild(node:Reference) = objectStore.get(node.hash)

  /**
   * The right and left subtree of this node.
   * - In case of a leaf node, these have to be created on demand
   * - In case of a reference node, these have to be retrieved from a block store
   * therefore this method returns a future
   */
  def split(node: Node): Future[(Node, Node)] = node match {
    case node: Data =>
      Future.successful(node.split)
    case node: Branch =>
      Future.successful(node.split)
    case node: Reference =>
      for {
        child <- getChild(node)
        (l, r) <- split(child)
        l <- wrap(l)
        r <- wrap(r)
      } yield (l, r)
  }

  final def toArray(node: Node): Future[Array[Long]] = {
    val target = new Array[Long](node.size.toInt)
    copyToArray(node, target, 0).map(_ => target)
  }

  /**
   * Converts all elements to an array. This will fail if the size of the tree is more than 2^32 elements.
   */
  def copyToArray(node: Node, target: Array[Long], offset: Int): Future[Unit] = node match {
    case node: Data =>
      Future.successful(node.copyToArray(target, offset))
    case node: Reference =>
      getChild(node).flatMap(child => copyToArray(child, target, offset))
    case node: Branch =>
      for {
        _ <- copyToArray(node.left, target, offset)
        _ <- copyToArray(node.right, target, offset + node.left.size.toInt)
      } yield ()
  }

  def fromLong(value: Long): Future[Node] = Future.successful(mkLeaf(Array[Long](value)))

  def fromLongs(values: Array[Long]): Future[Node] = {
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

  def merge(a: Node, b: Node): Future[Node] = {
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

  def weight(node:Node) =
    serializer.size(node)

  def wrap(a: Node): Future[Node] = {
    if (weight(a) < maxWeight)
      Future.successful(a)
    else
      objectStore.put(a).map { hash =>
        Reference(a.min, a.level, a.size, hash)
      }
  }
}
