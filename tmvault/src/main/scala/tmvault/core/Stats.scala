package tmvault.core

import scala.collection.immutable.{IntMap, LongMap}

case class Stats(leafSizes:LongMap[Int], leafHeights:IntMap[Int]) {
  def merge(that:Stats) : Stats = Stats(
    leafSizes = this.leafSizes.unionWith(that.leafSizes, (k, a, b) => a + b),
    leafHeights = this.leafHeights.unionWith(that.leafHeights, (k, a, b) => a + b))

  def histogramText = {
    val result = new StringBuilder()
    result.append("Leaf Sizes\n")
    for(i <- leafSizes.firstKey to leafSizes.lastKey)
      result.append(i + "\t" + "*" * (leafSizes.getOrElse(i, 0)) + "\n")
    result.append("Leaf Heights\n")
    for(i <- leafHeights.firstKey to leafHeights.lastKey)
      result.append(i + "\t" + "*" * leafHeights.getOrElse(i, 0) + "\n")
    result.toString
  }
}

object Stats {
  val empty = Stats(LongMap.empty, IntMap.empty)

  def apply(tree: IndexTree) : Stats = tree match {
    case leaf:IndexTree.Leaf =>
      empty.copy(
        leafSizes = empty.leafSizes + (leaf.size -> 1),
        leafHeights = empty.leafHeights + (java.lang.Long.bitCount(leaf.level - 1) -> 1))
    case branch:IndexTree.Branch =>
      val ls = apply(branch.left)
      val rs = apply(branch.right)
      ls merge rs
  }
}
