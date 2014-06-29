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
}
