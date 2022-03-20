package com.lzx.framework.utill

import org.apache.spark.SparkContext

object EvnUtil {

  private val Tlocal: ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]()

  def put(sc:SparkContext):Unit={
    Tlocal.set(sc)
  }

  def take():SparkContext={
     Tlocal.get()
  }

  def clear():Unit={
    Tlocal.remove()
  }
}
