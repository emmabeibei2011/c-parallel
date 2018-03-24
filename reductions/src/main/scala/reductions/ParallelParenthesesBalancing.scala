package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceHelper(chars: Array[Char], l: Int, r: Int): Boolean = {
      if (chars.isEmpty)
        if (l == r) true else false
      else if (chars.head == ')')
        if (l <= r) false else balanceHelper(chars.tail, l, r + 1)
      else if (chars.head == '(')
        balanceHelper(chars.tail, l + 1, r)
      else
        balanceHelper(chars.tail, l, r)
    }
    balanceHelper(chars, 0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    // For each sub, record the number of unmatched "(" and ")"
    def traverse(idx: Int, until: Int): (Int, Int) /*: ???*/ = {
      var leftUnmatched, rightUnmatched = 0
      for (i <- idx until until) {
        if (chars(i) == '(')
          leftUnmatched += 1
        else if (chars(i) == ')')
          if (leftUnmatched > 0) leftUnmatched -= 1
          else rightUnmatched += 1
      }
      (leftUnmatched, rightUnmatched)
    }

    def reduce(from: Int, until: Int): (Int, Int) /*: ???*/ = {
      if(until - from <= threshold) traverse(from, until)
      else {
        val middle = from + (until - from)/2
        val (left, right) = parallel(reduce(from, middle), reduce(middle, until))

        // When merging, unmatched ")" from L and unmatched "(" from R will not change
        // Only unmatched "(" from L and unmatched ")" can be merged and updated
        val matched = left._1 - right._2
        if (matched > 0)
          (right._1 + matched, left._2)
        else
          (right._1, left._2 - matched)
      }
    }
    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
