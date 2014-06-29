package tmvault.core
import org.junit._

class IndexTreeBuilderTest {
  implicit val context = IndexTreeContext(32)

  @Test
  def simpleTest() {
    val builder = new IndexTreeBuilder()
    val data = (0L until 1000L).toArray
    for(value <- data)
      builder += value
    val tree = builder.tree.get
    IndexTree.debugPrint(tree)
  }

  @Test
  def bulkAddTest() {
    val builder = new IndexTreeBuilder()
    val data = (0L until 1000L).toArray
    builder ++= data
    val tree = builder.tree.get
    IndexTree.debugPrint(tree)
  }

  @Test
  def randomTest() {
    val random = new scala.util.Random(0L)
    val data = (0 until 1000).map(_ => random.nextInt(100000).toLong).toArray.sorted.distinct
    val builder = new IndexTreeBuilder()
    builder ++= data
    val tree = builder.tree.get
    IndexTree.debugPrint(tree)
  }

  @Test
  def nonUniformRandomTest() {
    val random = new scala.util.Random(0L)
    val data = (0 until 1000000).map(_ => math.pow(10, random.nextDouble() *10).toLong).toArray.sorted.distinct
    val builder = new IndexTreeBuilder()(IndexTreeContext(32))
    builder ++= data
    val tree = builder.tree.get
    //IndexTree.debugPrint(tree)
    println(Stats(tree).histogramText)
  }

  @Test
  def leafInsertBugTest() {
    val test = Array[Long](
      755148,755232,755243,755295,755302,755314,755321,755331,755338,755351,755395,755398,755404,755456,755458,755459,
      755484,755493,755494,755513,755516,755524,755533,755535,755539,755556,755565,755566,755589,755594,755601,755615,
      755632,755652,755682,755716)
    val builder = new IndexTreeBuilder()(IndexTreeContext(32))
    builder ++= test
    val tree = builder.tree.get
    //IndexTree.debugPrint(tree)
    println(Stats(tree).histogramText)
  }

  @Test
  def fullMergeTest() {
    val random = new scala.util.Random(0L)
    val data1 = (0 until 1000).map(_ => random.nextInt(100000).toLong).toArray.sorted.distinct
    val data2 = (0 until 1000).map(_ => random.nextInt(1000000).toLong).toArray.sorted.distinct
    val builder1 = new IndexTreeBuilder()
    builder1 ++= data1
    val tree1 = builder1.tree.get
    val builder2 = new IndexTreeBuilder()
    builder2 ++= data2
    val tree2 = builder2.tree.get
    val merged1 =tree1 merge tree2
    val merged2 =tree2 merge tree1
    IndexTree.debugPrint(merged1)
    println(Stats(merged1).histogramText)
    println(merged1.size - tree1.size - tree2.size)
  }
}
