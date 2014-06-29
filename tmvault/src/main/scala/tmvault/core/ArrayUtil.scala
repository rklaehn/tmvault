package tmvault.core

private[core] object ArrayUtil {

  def isSorted(data: Array[Long], from:Int, until:Int): Boolean = {
    var i = from
    while (i < until - 1) {
      if (data(i) > data(i + 1))
        return false
      i += 1
    }
    return true
  }

  def isIncreasing(data: Array[Long], from:Int, until:Int): Boolean = {
    var i = from
    while (i < until - 1) {
      if (data(i) >= data(i + 1))
        return false
      i += 1
    }
    return true
  }

  def firstIndexWhereGE(data: Array[Long], from: Int, until: Int, value: Long): Int = {
    var i = from
    while (i < until) {
      if (data(i) >= value)
        return i
      i += 1
    }
    return i
  }

  def removeDuplicates(data: Array[Long], from: Int, until: Int): Int =
    if (data.length < 2)
      data.length
    else {
      require(isSorted(data, from, until))
      var i = from
      var j = from
      while (i < until) {
        if (data(i) != data(j)) {
          j += 1
          data(j) = data(i)
        }
        i += 1
      }
      require(isIncreasing(data, from, j + 1))
      j + 1
    }
}
