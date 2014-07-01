package tmvault.core

private[core] object ArrayUtil {

  implicit class ArrayOps(val data: Array[Long]) extends AnyVal {

    def concat(that: Array[Long]): Array[Long] = {
      val result = new Array[Long](data.length + that.length)
      System.arraycopy(data, 0, result, 0, data.length)
      System.arraycopy(that, 0, result, data.length, that.length)
      result
    }

    def sortedAndDistinct: Array[Long] = {
      if (data.length < 2)
        data
      else {
        val from = 0
        val until = data.length
        java.util.Arrays.sort(data)
        var i = from
        var j = from
        while (i < until) {
          if (data(i) != data(j)) {
            j += 1
            data(j) = data(i)
          }
          i += 1
        }
        data.takeUnboxed(j + 1)
      }
    }

    def takeUnboxed(n: Int) = {
      if (n <= 0)
        new Array[Long](0)
      else if (n >= data.length)
        data
      else
        slice(0, n)
    }

    def dropUnboxed(n: Int) = {
      if (n <= 0)
        data
      else if (n >= data.length)
        new Array[Long](0)
      else
        slice(n, data.length)
    }

    private def slice(from: Int, until: Int): Array[Long] = {
      val result = new Array[Long](until - from)
      System.arraycopy(data, from, result, 0, until - from)
      result
    }

    def isSorted(from: Int = 0, until: Int = data.length): Boolean = {
      var i = from
      while (i < until - 1) {
        if (data(i) > data(i + 1))
          return false
        i += 1
      }
      return true
    }

    def isIncreasing(from: Int = 0, until: Int = data.length): Boolean = {
      var i = from
      while (i < until - 1) {
        if (data(i) >= data(i + 1))
          return false
        i += 1
      }
      return true
    }

    def firstIndexWhereGE(value: Long, from: Int = 0, until: Int = data.length): Int = {
      var i = from
      while (i < until) {
        if (data(i) >= value)
          return i
        i += 1
      }
      return i
    }

  }

  def isSorted(data: Array[Long], from: Int, until: Int): Boolean = {
    var i = from
    while (i < until - 1) {
      if (data(i) > data(i + 1))
        return false
      i += 1
    }
    return true
  }

  def isIncreasing(data: Array[Long], from: Int, until: Int): Boolean = {
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
