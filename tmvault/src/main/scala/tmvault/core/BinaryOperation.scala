package tmvault.core

abstract class BinaryOperation[T] extends ((IndexTree, IndexTree) => T) {

  import tmvault.core.IndexTree._

  def apply(a: IndexTree, b: IndexTree): T = {
    a match {
      case a: Branch =>
        b match {
          case b: Branch => op(a, b)
          case b: Leaf => op(a, b)
        }
      case a: Leaf =>
        b match {
          case b: Branch => op(a, b)
          case b: Leaf => op(a, b)
        }
    }
  }

  protected def op(a: Branch, b: Branch): T

  protected def op(a: Leaf, b: Leaf): T

  protected def op(a: Branch, b: Leaf): T

  protected def op(a: Leaf, b: Branch): T
}
