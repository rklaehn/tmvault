package tmvault.eager

import org.junit.Assert._
import org.junit.Test
import tmvault.util.ArrayUtil

import scala.concurrent.{Future, Await, ExecutionContext}

class IndexTreeTest {

  @Test
  def testLeafCreation(): Unit = {
    val leaf1 = IndexTree.fromLong(12345L)
    assertEquals(1, leaf1.width)
    assertEquals(12345L, leaf1.min)

    val leaf2 = IndexTree.fromLong(100000L)
    assertEquals(1, leaf2.width)
    assertEquals(100000L, leaf2.min)

    val leaf3 = IndexTree.mkLeaf(Array[Long](0x0, 0xF))
    assertEquals(16, leaf3.width)
    assertEquals(0, leaf3.min)
  }

  def compareCreation(data: Array[Long]) = {
    import scala.concurrent.duration._
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    implicit val c = IndexTreeContext(ExecutionContext.global, null, 32)
    import c.executionContext
    val random = new scala.util.Random(0)
    val shuffled = random.shuffle(data.toIndexedSeq).toArray
    def combine(a:Future[IndexTree], b:Future[IndexTree]) : Future[IndexTree] =
      for (a <- a; b <- b; m <- IndexTree.merge(a, b))
      yield m
    def reduceLeft(elems: Array[Long]) = elems.map(x => Future.successful(IndexTree.fromLong(x))).reduceLeft(combine)
    def reduceRight(elems: Array[Long]) = elems.map(x => Future.successful(IndexTree.fromLong(x))).reduceRight(combine)
    val tree1 = Await.result(reduceLeft(data), 1.minute)
    val tree2 = Await.result(reduceLeft(shuffled), 1.minute)
    val tree3 = Await.result(reduceRight(data), 1.minute)
    val tree4 = Await.result(reduceRight(shuffled), 1.minute)
    val tree5 = Await.result(IndexTree.fromLongs(data), 1.minute)
    assertEquals(tree1, tree2)
    assertEquals(tree1, tree3)
    assertEquals(tree1, tree4)
    assertEquals(tree1, tree5)
  }

  @Test
  def testIncrementalTreeCreation() : Unit = {
    val consecutive =
      (0L until 1024L).toArray
    val random = {
      val random = new scala.util.Random(0)
      (0 until 10000).map(_ => random.nextInt(100000).toLong).toArray.sorted.distinct
    }
    val nonUniformRandom = {
      val random = new scala.util.Random(0)
      (0 until 1000000).map(_ => math.pow(10, random.nextDouble() *10).toLong).toArray.sorted.distinct
    }
    val test = Seq(
      nonUniformRandom,
      consecutive,
      random
    )
    for(data <- test)
      compareCreation(data)
  }
}
