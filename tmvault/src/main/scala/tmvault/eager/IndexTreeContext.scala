package tmvault.eager

import tmvault.io.ObjectStore

import scala.concurrent.ExecutionContext

/**
 * Collects everything that eager IndexTree operations need to operate
 * @param ec the execution context
 * @param store the object store to store and load objects (tree nodes in this case)
 * @param maxValues the maximum number of values (longs) we want in a leaf
 */
case class IndexTreeContext(ec:ExecutionContext, store:ObjectStore[IndexTree], maxValues:Int, maxWeight:Int) {
  implicit def executionContext = ec

}