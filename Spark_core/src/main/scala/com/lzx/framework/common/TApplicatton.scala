package com.lzx.framework.common

import com.lzx.framework.controller.WordCountController
import com.lzx.framework.utill.EvnUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplicatton {

 // op: =>Unit 无入参无返回值的函数
 def start( master:String= "local[*]", appname:String="HotCategoryTop10Analysis")(op: =>Unit): Unit ={
   val sparConf = new SparkConf().setMaster(master).setAppName(appname)
   val sc = new SparkContext(sparConf)
   EvnUtil.put(sc)
   try {
     op
   }
   catch {
     case ex=> println(println(ex.getMessage))
   }
   sc.stop()
   EvnUtil.clear()
 }
}
