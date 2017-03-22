package com.utilities

/**
  * Created by davgutavi on 22/03/17.
  */
object TimingUtils {

  def time[T](block: => T): T = {
    val start = System.currentTimeMillis
    val res = block
    val totalTime = System.currentTimeMillis - start
    println("Elapsed time: %1d ms".format(totalTime))
    res
  }

}
