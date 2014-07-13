package tmvault.eager

import java.io.File
import java.nio.file.Files

import org.junit.Assert._
import org.junit.Test
import tmvault.io._
import tmvault.util.ArrayUtil

import tmvault.{Future, Await, ExecutionContext}
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import ArrayUtil._

object TestData {

  lazy val consecutive =
    (0L until 1024L).toArray

  lazy val random = {
    val random = new scala.util.Random(0)
    (0 until 10000).map(_ => random.nextInt(100000).toLong).toArray.sorted.distinct
  }

  def nonUniformRandom(size: Int) = {
    val random = new scala.util.Random(0)
    (0 until size).map(_ => math.pow(10, random.nextDouble() * 10).toLong).toArray
  }

  lazy val nonUniformRandom100000 = nonUniformRandom(100000).sorted.distinct

  lazy val nonUniformRandom10000000 = nonUniformRandom(10000000).sorted.distinct

  def nonUniformRandomHuge(n: Long): Iterator[Long] = {
    val random = new scala.util.Random(0)
    var x = 0L
    var dx = 0.5
    val ddx = 0.5
    def next = {
      dx = (dx + (random.nextDouble() * 2 - 1) * 0.1).max(0.0).min(1.0)
      x += math.pow(10, dx * 4).round.toLong
      x
    }
    val result = (0L until n).toIterator.map(_ => next)
    result
  }
}

class IndexTreeTest {

  implicit class FutureGet[T](val f:Future[T]) {
    def get:T = Await.result(f, 1.hour)
  }

  def time[T](text:String)(action: => T) : T = {
    val t0 = System.nanoTime()
    val result = action
    val dt = System.nanoTime() - t0
    println(text +" took " + dt / 1e9)
    result
  }

  import TestData._

  val timeout = 1.hour

  @Test
  def testLeafCreation(): Unit = {
    val leaf1 = Node.mkLeaf(Array(12345L))
    assertEquals(1, leaf1.width)
    assertEquals(12345L, leaf1.min)

    val leaf2 = Node.mkLeaf(Array(100000L))
    assertEquals(1, leaf2.width)
    assertEquals(100000L, leaf2.min)

    val leaf3 = Node.mkLeaf(Array[Long](0x0, 0xF))
    assertEquals(16, leaf3.width)
    assertEquals(0, leaf3.min)
  }

  def areEqual(a: Node, b: Node): Boolean = {
    a == b
  }

  def compareCreation(data: Array[Long]) = {
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val blockStore = InMemoryBlockStore.create
    val tree = IndexTree.create(blockStore, maxValues = 32, maxBytes = 42 * 4)
    val random = new scala.util.Random(0)
    val shuffled = random.shuffle(data.toIndexedSeq).toArray
    def combine(a: Future[Node], b: Future[Node]): Future[Node] =
      for (a <- a; b <- b; m <- tree.merge(a, b))
      yield m
    def reduceLeft(elems: Array[Long]) = elems.map(tree.fromLong).reduceLeft(combine)
    def reduceRight(elems: Array[Long]) = elems.map(tree.fromLong).reduceRight(combine)
    val tree1 = reduceLeft(data).get
    val tree2 = reduceLeft(shuffled).get
    val tree3 = reduceRight(data).get
    val tree4 = reduceRight(shuffled).get
    val tree5 = tree.fromLongs(data).get
    assertEquals(tree1, tree2)
    assertEquals(tree1, tree3)
    assertEquals(tree1, tree4)
    assertEquals(tree1, tree5)
    println(tree.objectStore)
  }

  def createQuick(data: Array[Long]) = {
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val blockStore = InMemoryBlockStore.create
    val tree = IndexTree.create(blockStore, maxValues = 32, maxBytes = 32768)
    val node = tree.fromLongs(data).get
    val data2 = tree.toArray(node).get
    assertArrayEquals(data, data2)
    println(tree.objectStore)
  }

  def delTree(file:File) : Unit = {
    try {
      import scala.sys.process._
      s"rm -rf $file".!!
    } catch {
      case e:RuntimeException =>
        println(s"Unable to delete $file")
    }
  }

  def showAndDel(file:File) : Unit = {
    try {
      import scala.sys.process._
      println(s"du -h $file".!!)
      s"rm -rf $file".!!
    } catch {
      case e:RuntimeException =>
        println(s"Unable to delete $file")
    }
  }

  def createOnDisk(data: Array[Long]) = {
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val file = Files.createTempDirectory("leveldb").toFile
    val blockStore = LevelDBBlockStore.create(file)
    val tree = IndexTree.create(blockStore, maxValues = 32, maxBytes = 32768)
    val node = tree.fromLongs(data).get
    val data2 = tree.toArray(node).get
    assertArrayEquals(data, data2)
    println(tree.objectStore)
    delTree(file)
  }

  def createFromStream(data: Iterator[Long], chunk: Int = 10000, useCache:Boolean = false) : File = {
    val file = Files.createTempDirectory("leveldb").toFile
    val blockStore = LevelDBBlockStore.create(file)
    val tree = IndexTree.create(blockStore, maxValues = 32, maxBytes = 32768, useCache = useCache)
    val chunks = data
      .grouped(chunk)
      .map { x =>
        val res = x.toArray.sortedAndDistinct;
        // println(res.head + " " + res.last);
        res
      }
      .map(tree.fromLongs _)
    def combine(a: Future[Node], b: Future[Node]): Future[Node] = {
      //println("combine")
      for {
        a <- a
        b <- b
        m <- {
          //println(s"merge ${a.size} ${b.size}")
          tree.merge(a, b)
        }
      } yield m
    }
    val result = chunks.reduceLeft(combine)
    val node = result.get
    val x = tree.hashes(node).get
    blockStore.close()
    println(node.size)
    println(x.size)
    file
  }

  @Test
  def testIncrementalTreeCreation(): Unit = {
    val test = Seq(
      consecutive,
      random,
      nonUniformRandom100000
    )
    for (data <- test)
      compareCreation(data)
  }

  @Test
  def testQuickTreeCreation(): Unit = {
    createQuick(nonUniformRandom10000000)
  }

  @Test
  def testChunkedRandom(): Unit = {
    val blockStore = InMemoryBlockStore.create
    val tree = IndexTree.create(blockStore, maxValues = 32, maxBytes = 32768 * 1000)
    val chunks: Array[Array[Long]] = nonUniformRandom(10000).grouped(100).map(_.sortedAndDistinct).toArray
    val trees = chunks.map(tree.fromLongs)
    def combine(a: Future[Node], b: Future[Node]): Future[Node] =
      for (a <- a; b <- b; m <- tree.merge(a, b))
      yield m
    val node1 = trees.reduceLeft(combine)
    val node2 = trees.reduceRight(combine)
    val a = node1.get
    val b = node2.get
    val x = tree.hashes(a).get
    val aa = tree.toArray(a).get
    val ba = tree.toArray(b).get
    assertArrayEquals(aa, ba)
    if(a != b) {
      tree.show(a)()
      tree.show(b)()
      val ad = tree.dataLeafs(a).get
      val bd = tree.dataLeafs(b).get
      val adc = ad.map(_.length).toIndexedSeq
      val bdc = bd.map(_.length).toIndexedSeq
      val zipped = ad.zip(bd).zipWithIndex
      val diff = zipped.dropWhile { case ((a,b),i) => a == b }
      println(diff.head)
      require(a == b)
    }
    println(a.size)
    println(x.size)
    println(blockStore)
  }

  @Test
  def testChunkedRandomCached(): Unit = {
    val blockStore = InMemoryBlockStore.create
    val tree = IndexTree.create(blockStore, maxValues = 32, maxBytes = 32768, useCache = true)
    val chunks: Array[Array[Long]] = nonUniformRandom(100000).grouped(1000).map(_.sortedAndDistinct).toArray
    val trees = chunks.map(tree.fromLongs)
    def combine(a: Future[Node], b: Future[Node]): Future[Node] =
      for (a <- a; b <- b; m <- tree.merge(a, b))
      yield m
    val node1 = trees.reduceLeft(combine)
    val node2 = trees.reduceRight(combine)
    val a = node1.get
    val b = node2.get
    val aa = tree.toArray(a).get
    val ba = tree.toArray(b).get
    assertArrayEquals(aa, ba)
    //    if(a != b)
    //      require(a == b)
  }

  @Test
  def testOnDiskCreation(): Unit = {
    createOnDisk(nonUniformRandom10000000)
  }

  @Test
  def testOnDiskCreationHuge(): Unit = {
    import scala.sys.process._
    showAndDel(time("testOnDiskCreationHuge") {
      createFromStream(nonUniformRandomHuge(10000000))
    })
    showAndDel(time("testOnDiskCreationHuge cached") {
      createFromStream(nonUniformRandomHuge(10000000), useCache = true)
    })
  }
}
