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
    println(merged1.size - tree1.size - tree2.size)
  }
}
