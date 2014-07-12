package tmvault.eager

import java.io.File
import java.nio.file.Files

import org.junit.Assert._
import org.junit.Test
import tmvault.io.{ObjectStore, LevelDBBlockStore, InMemoryBlockStore}
import tmvault.util.ArrayUtil

import tmvault.{Future, Await}
import scala.concurrent.{ExecutionContext}

class IndexTreeTest {

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

  def areEqual(a:Node, b:Node) : Boolean = {
    a == b
  }

  def compareCreation(data: Array[Long]) = {
    import scala.concurrent.duration._
    val timeout = 1.hour
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val blockStore = InMemoryBlockStore.create(ExecutionContext.global)
    val objectStore = ObjectStore(blockStore, IndexTreeSerializer)(ExecutionContext.global)
    implicit val c = IndexTreeContext(ExecutionContext.global, objectStore, 32, 41*4)
    import c.executionContext
    val random = new scala.util.Random(0)
    val shuffled = random.shuffle(data.toIndexedSeq).toArray
    def combine(a:Future[Node], b:Future[Node]) : Future[Node] =
      for (a <- a; b <- b; m <- Node.merge(a, b))
      yield m
    def reduceLeft(elems: Array[Long]) = elems.map(Node.fromLong).reduceLeft(combine)
    def reduceRight(elems: Array[Long]) = elems.map(Node.fromLong).reduceRight(combine)
    val tree1 = Await.result(reduceLeft(data), timeout)
    val tree2 = Await.result(reduceLeft(shuffled), timeout)
    val tree3 = Await.result(reduceRight(data), timeout)
    val tree4 = Await.result(reduceRight(shuffled), timeout)
    val tree5 = Await.result(Node.fromLongs(data), timeout)
    assertEquals(tree1, tree2)
    assertEquals(tree1, tree3)
    assertEquals(tree1, tree4)
    assertEquals(tree1, tree5)
    println(objectStore)
  }

  def createQuick(data: Array[Long]) = {
    import scala.concurrent.duration._
    val timeout = 1.hour
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val blockStore = InMemoryBlockStore.create(ExecutionContext.global)
    val objectStore = ObjectStore(blockStore, IndexTreeSerializer)(ExecutionContext.global)
    implicit val c = IndexTreeContext(ExecutionContext.global, objectStore, 32768/8, 32)
    val tree = Await.result(Node.fromLongs(data), timeout)
    val data2 = Await.result(Node.toArray(tree), timeout)
    assertArrayEquals(data, data2)
  }

  def createOnDisk(data: Array[Long]) = {
    import scala.concurrent.duration._
    val timeout = 1.hour
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val file = Files.createTempDirectory("leveldb").toFile
    val blockStore = LevelDBBlockStore.create(file)
    val objectStore = ObjectStore(blockStore, IndexTreeSerializer)(ExecutionContext.global)
    implicit val c = IndexTreeContext(ExecutionContext.global, objectStore, 32768/8, 32)
    val tree = Await.result(Node.fromLongs(data), timeout)
    val data2 = Await.result(Node.toArray(tree), timeout)
    assertArrayEquals(data, data2)
    println(objectStore)
  }

  @Test
  def testIncrementalTreeCreation() : Unit = {
    val consecutive =
      (0L until 1024L).toArray
    val random = {
      val random = new scala.util.Random(0)
      (0 until 10000).map(_ => random.nextInt(100000).toLong).toArray.sorted.distinct
    }
    val nonUniformRandomShort = {
      val random = new scala.util.Random(0)
      (0 until 100000).map(_ => math.pow(10, random.nextDouble() *10).toLong).toArray.sorted.distinct
    }
    val nonUniformRandomLong = {
      val random = new scala.util.Random(0)
      (0 until 10000000).map(_ => math.pow(10, random.nextDouble() *10).toLong).toArray.sorted.distinct
    }
    val test = Seq(
      consecutive,
      random,
      nonUniformRandomShort
    )
    createOnDisk(nonUniformRandomLong)
    createQuick(nonUniformRandomLong)
    for(data <- test)
      compareCreation(data)
  }
}
