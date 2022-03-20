package com.lzx.framework.applocation

import com.lzx.framework.common.TApplicatton
import com.lzx.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApplocaton extends App with TApplicatton{

  start() ({
    val controller: WordCountController = new WordCountController
    controller.dispatch()
  })

}
