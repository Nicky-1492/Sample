package com.njkol.spark.demo

/**
 * Input: (2 -> 4 -> 3) + (5 -> 6 -> 4)
 * Output: 7 -> 0 -> 8
 * Explanation: 342 + 465 = 807.
 */

object Sample extends App { 
  val x = Array(2, 4, 3)
  val y = Array(5, 6, 4)
  var carriage = 0
  var temp1 = 0
  var temp2 = 0
  val z = for {
    i <- 0 to (x.length - 1)
    j <- 0 to (y.length - 1)
    if (i == j)
  } yield {
    temp2 = carriage
    if ((x.apply(i) + y.apply(j)) >= 10) {
      temp1 = (x.apply(i) + y.apply(j)) / 10
    }
    carriage = temp1
    (x.apply(i) + y.apply(j)) % 10 + temp2

  }

  z.foreach(println)
}