//package tmvault.core
//import scala.annotation.switch
//
//class Merge(implicit context:IndexTreeContext) extends BinaryOperation[IndexTree] {
//  import tmvault.core.BitUtil._
//  import tmvault.core.IndexTree._
//  import Merge._
//
//  override def apply(a: IndexTree, b: IndexTree): IndexTree = {
//    if (a.size + b.size < context.maxValues)
//      mkLeaf(a, b)
//    else if (disjoint(a, b))
//      mkBranch(a, b)
//    else
//      super.apply(a, b)
//  }
//
//  protected override def op(a: Branch, b: Branch): IndexTree = (unsignedCompare(a.level, b.level): @switch) match {
//    case SameLevel =>
//      mkBranch(apply(a.left, b.left), apply(a.right, b.right))
//    case AAboveB =>
//      if(zero(b.prefix, a.level))
//        mkBranch(apply(a.left, b), a.right)
//      else
//        mkBranch(a.left, apply(a.right, b))
//    case BAboveA =>
//      if(zero(a.prefix, b.level))
//        mkBranch(apply(a, b.left), b.right)
//      else
//        mkBranch(b.left, apply(a, b.right))
//  }
//
//  protected override def op(a: Branch, b: Leaf): IndexTree = (unsignedCompare(a.level, b.level): @switch) match {
//    case SameLevel =>
//      val (b_left, b_right) = b.split
//      mkBranch(apply(a.left, b_left), apply(a.right, b_right))
//    case AAboveB =>
//      if(zero(b.prefix, a.level))
//        mkBranch(apply(a.left, b), a.right)
//      else
//        mkBranch(a.left, apply(a.right, b))
//    case BAboveA =>
//      val (b_left, b_right) = b.split
//      if(zero(a.prefix, b.level))
//        mkBranch(apply(a, b_left), b_right)
//      else
//        mkBranch(b_left, apply(a, b_right))
//  }
//
//  protected override def op(a: Leaf, b: Branch): IndexTree = (unsignedCompare(a.level, b.level): @switch) match {
//    case SameLevel =>
//      val (a_left, a_right) = a.split
//      mkBranch(apply(a_left, b.left), apply(a_right, b.right))
//    case AAboveB =>
//      val (a_left, a_right) = a.split
//      if(zero(b.prefix, a.level))
//        mkBranch(apply(a_left, b), a_right)
//      else
//        mkBranch(a_left, apply(a_right, b))
//    case BAboveA =>
//      if(zero(a.prefix, b.level))
//        mkBranch(apply(a, b.left), b.right)
//      else
//        mkBranch(b.left, apply(a, b.right))
//  }
//
//  protected override def op(a: Leaf, b: Leaf): IndexTree = (unsignedCompare(a.level, b.level): @switch) match {
//    case SameLevel =>
//      val (a_left, a_right) = a.split
//      val (b_left, b_right) = b.split
//      mkBranch(apply(a_left, b_left), apply(a_right, b_right))
//    case AAboveB =>
//      val (a_left, a_right) = a.split
//      if(zero(b.prefix, a.level))
//        mkBranch(apply(a_left, b), a_right)
//      else
//        mkBranch(a_left, apply(a_right, b))
//    case BAboveA =>
//      val (b_left, b_right) = b.split
//      if(zero(a.prefix, b.level))
//        mkBranch(apply(a, b_left), b_right)
//      else
//        mkBranch(b_left, apply(a, b_right))
//  }
//}
//
//object Merge {
//  private final val AAboveB = 1
//  private final val BAboveA = -1
//  private final val SameLevel = 0
//}