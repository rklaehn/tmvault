package tmvault.eager

import tmvault.io.BlockStore

import scala.concurrent.ExecutionContext

/**
 * Collects everything that eager IndexTree operations need to operate
 * @param ec the execution context
 * @param blockStore the block store to store and load blocks
 * @param maxValues the maximum number of values (longs) we want in a leaf
 */
case class IndexTreeContext(ec:ExecutionContext, blockStore:BlockStore, maxValues:Int, maxWeight:Int) {
  implicit def executionContext = ec

}