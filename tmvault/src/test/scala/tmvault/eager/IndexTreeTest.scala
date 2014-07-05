package tmvault.eager

import org.junit.Assert._
import org.junit.Test
import tmvault.io.InMemoryBlockStore
import tmvault.util.ArrayUtil

import tmvault.{Future, Await}
import scala.concurrent.{ExecutionContext}

class IndexTreeTest {

  @Test
  def testLeafCreation(): Unit = {
    val leaf1 = IndexTree.mkLeaf(Array(12345L))
    assertEquals(1, leaf1.width)
    assertEquals(12345L, leaf1.min)

    val leaf2 = IndexTree.mkLeaf(Array(100000L))
    assertEquals(1, leaf2.width)
    assertEquals(100000L, leaf2.min)

    val leaf3 = IndexTree.mkLeaf(Array[Long](0x0, 0xF))
    assertEquals(16, leaf3.width)
    assertEquals(0, leaf3.min)
  }

  def areEqual(a:IndexTree, b:IndexTree) : Boolean = {
    a == b
  }

  def compareCreation(data: Array[Long]) = {
    import scala.concurrent.duration._
    val timeout = 1.hour
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val store = InMemoryBlockStore.create(ExecutionContext.global)
    implicit val c = IndexTreeContext(ExecutionContext.global, store, 32, 32)
    import c.executionContext
    val random = new scala.util.Random(0)
    val shuffled = random.shuffle(data.toIndexedSeq).toArray
    def combine(a:Future[IndexTree], b:Future[IndexTree]) : Future[IndexTree] =
      for (a <- a; b <- b; m <- IndexTree.merge(a, b))
      yield m
    def reduceLeft(elems: Array[Long]) = elems.map(IndexTree.fromLong).reduceLeft(combine)
    def reduceRight(elems: Array[Long]) = elems.map(IndexTree.fromLong).reduceRight(combine)
    val tree1 = Await.result(reduceLeft(data), timeout)
    val tree2 = Await.result(reduceLeft(shuffled), timeout)
    val tree3 = Await.result(reduceRight(data), timeout)
    val tree4 = Await.result(reduceRight(shuffled), timeout)
    val tree5 = Await.result(IndexTree.fromLongs(data), timeout)
    assertEquals(tree1, tree2)
    assertEquals(tree1, tree3)
    assertEquals(tree1, tree4)
    assertEquals(tree1, tree5)
    println(store)
  }

  def createQuick(data: Array[Long]) = {
    import scala.concurrent.duration._
    val timeout = 1.hour
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val store = InMemoryBlockStore.create(ExecutionContext.global)
    implicit val c = IndexTreeContext(ExecutionContext.global, store, 32, 32)
    import c.executionContext
    val tree5 = Await.result(IndexTree.fromLongs(data), timeout)
    println(store)
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
    createQuick(nonUniformRandomLong)
    for(data <- test)
      compareCreation(data)
  }
}
