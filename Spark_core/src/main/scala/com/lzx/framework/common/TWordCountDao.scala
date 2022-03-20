package com.lzx.framework.common

import com.lzx.framework.utill.EvnUtil
import org.apache.spark.SparkContext

trait TWordCountDao {
  private val sc: SparkContext = EvnUtil.take()
  def getDatas(path:String)=sc.textFile(path)

}
