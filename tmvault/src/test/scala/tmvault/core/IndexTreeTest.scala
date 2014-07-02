package tmvault.core

import org.junit.Test
import org.junit.Assert._

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
    require(ArrayUtil.isIncreasing(data, 0, data.length))
    val random = new scala.util.Random(0)
    val shuffled = random.shuffle(data.toIndexedSeq).toArray
    val tree1 = data.map(IndexTree.fromLong).reduce(IndexTree.merge)
    val tree2 = shuffled.map(IndexTree.fromLong).reduce(IndexTree.merge)
    val tree3 = data.map(IndexTree.fromLong).reduceRight(IndexTree.merge)
    val tree4 = shuffled.map(IndexTree.fromLong).reduceRight(IndexTree.merge)
    val tree5 = IndexTree.fromLongs(data)
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
