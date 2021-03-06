package tmvault.eager

import tmvault.io.{CachingObjectStore, BlockStore, BlobSerializer, ObjectStore}
import tmvault.util.SHA1Hash

import tmvault.{Future, ExecutionContext}
import tmvault.util.ArrayUtil._
import java.lang.Long.{bitCount, highestOneBit}

import scala.collection.immutable.HashSet

object IndexTree {

  def create(blockStore: BlockStore, maxValues:Int = 32, maxBytes:Int = 32768, useCache:Boolean = false)(implicit ec:ExecutionContext) : IndexTree =
    new SimpleIndexTree(maxValues, maxBytes, useCache, blockStore, ec)

  private case class SimpleIndexTree(maxValues:Int, maxBytes:Int, useCache:Boolean, blockStore: BlockStore, executionContext:ExecutionContext) extends IndexTree {

    override val serializer: BlobSerializer[Node] = IndexTreeSerializer(this)

    override val objectStore: ObjectStore[Node] = {
      if(useCache)
        CachingObjectStore(
          ObjectStore(blockStore, serializer)(executionContext)
        )
      else
        ObjectStore(blockStore, serializer)(executionContext)
    }
  }
}

abstract class IndexTree {
  import Node.overlap

  /**
   * The maximum number of longs before a leaf node is split
   */
  def maxValues : Int

  /**
   * The maximum number of serialized bytes before a tree is wrapped in a reference
   */
  def maxBytes : Int

  /**
   * The serializer that is used for calculating sizes. This should be the same one that is used for serialising values
   */
  def serializer: BlobSerializer[Node]

  /**
   * The object store to resolve and store references with
   */
  def objectStore: ObjectStore[Node]

  implicit def executionContext : ExecutionContext

  def getChild(node:Reference) : Future[Node] = objectStore.get(node.hash)

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

  def fromLongsNow(values: Array[Long]): Node = {
    require(values.length > 0)
    require(values.isIncreasing())
    def mkNode(from: Int, until: Int): Node = {
      if (until - from <= maxValues)
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
        mergeNow(left, right)
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

  def concatNow(a:Node, b:Node) : Node = {
    val temp = new Array[Long]((a.size + b.size).toInt)
    a.copyToArray(temp, 0)
    b.copyToArray(temp, a.size.toInt)
    mkLeaf(temp.sortedAndDistinct)
  }

  def mergeNonOverlappingNow(a:Node, b:Node) = {
    if (a.size + b.size <= maxValues && !a.containsReferences && !b.containsReferences) {
      concatNow(a,b)
    } else
      Node.mergeNonOverlapping(a,b)
  }

  def mkLeaf(data:Array[Long]) = {
    require(data.length <= maxValues)
    Node.mkLeaf(data)
  }

  private[eager] def mergeNow(a: Node, b: Node): Node = {
    if (a.size + b.size <= maxValues && !a.containsReferences && !b.containsReferences) {
      // we don't care if they overlap or not, since they have to be merged anyway
      concatNow(a,b)
    } else if (!overlap(a, b)) {
      // the two nodes do not overlap, so we can just create a branch node above them
      mergeNonOverlappingNow(a, b)
    } else if (a.level > b.level) {
      // a is above b
      val (l, r) = a.split
      if (b.min < a.center)
        mergeNonOverlappingNow(mergeNow(l, b), r)
      else
        mergeNonOverlappingNow(l, mergeNow(r, b))
    } else if (a.level < b.level) {
      // b is above a
      val (l, r) = b.split
      if (a.min < b.center)
        mergeNonOverlappingNow(mergeNow(a, l), r)
      else
        mergeNonOverlappingNow(l, mergeNow(a, r))
    } else {
      // a and b have the same interval
      if (a.level == 0)
        a
      else {
        val (al, ar) = a.split
        val (bl, br) = b.split
        mergeNonOverlappingNow(mergeNow(al, bl), mergeNow(ar, br))
      }
    }
  }

  /**
   * Enumerates all reference hashes of this tree
   */
  def hashes(node:Node) : Future[HashSet[SHA1Hash]] = node match {
    case node:Data =>
      Future.successful(HashSet.empty[SHA1Hash])
    case node:Reference =>
      for {
        child <- getChild(node)
        h <- hashes(child)
      } yield h + node.hash
    case node:Branch =>
      for {
        l <- hashes(node.left)
        r <- hashes(node.right)
      } yield r union l
  }

  def dataLeafs(node:Node) : Future[IndexedSeq[IndexedSeq[Long]]] = node match {
    case node:Data =>
      Future.successful(IndexedSeq(node.data))
    case node:Reference =>
      for {
        child <- getChild(node)
        leafs <- dataLeafs(child)
      } yield leafs
    case node:Branch =>
      for {
        l <- dataLeafs(node.left)
        r <- dataLeafs(node.right)
      } yield l ++ r
  }

  def mergeNonOverlapping(a: Node, b:Node) : Future[Node] = {
    if (a.size + b.size <= maxValues) {
      val temp = new Array[Long]((a.size + b.size).toInt)
      for {
        _ <- copyToArray(a, temp, 0)
        _ <- copyToArray(b, temp, a.size.toInt)
        r <- wrap(mkLeaf(temp.sortedAndDistinct))
      } yield r
    } else
      wrap(mergeNonOverlappingNow(a,b))
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
        r <- mergeNonOverlapping(a, b)
      } yield r
    } else if (a.level > b.level) {
      // a is above b
      split(a).flatMap { case (l, r) =>
        if (b.min < a.center)
          merge(l, b).flatMap(l => mergeNonOverlapping(l, r))
        else
          merge(r, b).flatMap(r => mergeNonOverlapping(l, r))
      }
    } else if (a.level < b.level) {
      // b is above a
      split(b).flatMap { case (l, r) =>
        if (a.min < b.center)
          merge(a, l).flatMap(l => mergeNonOverlapping(l, r))
        else
          merge(a, r).flatMap(r => mergeNonOverlapping(l, r))
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
          r <- merge(ar, br)
          result <- mergeNonOverlapping(l, r)
        } yield result
      }
    }
  }

  def weight(node:Node) =
    serializer.size(node)

  def wrap(a: Node): Future[Node] = {
    if(serializer.size(a) <= maxBytes)
    // if (a.bytes <= maxBytes)
      Future.successful(a)
    else
      objectStore.put(a).map { hash =>
        Reference(a.min, a.level, a.size, hash)
      }
  }
}
