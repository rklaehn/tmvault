package tmvault.io

import tmvault.util.SHA1Hash
import tmvault.{Future, ExecutionContext}

trait BlockStore {
  
  def putBytes(data: Array[Byte]): Future[SHA1Hash]

  def getBytes(key: SHA1Hash): Future[Array[Byte]]
}



