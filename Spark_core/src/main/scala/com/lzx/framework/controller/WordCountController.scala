package com.lzx.framework.controller

import com.lzx.framework.common.TWordCountController
import com.lzx.framework.server.WordCountServer
import org.apache.spark.rdd.RDD

class WordCountController extends TWordCountController {

  private val server: WordCountServer = new WordCountServer
  def dispatch()={
    server.dataAnalysis()
    // TODO 计算单跳转换率
    // 分子除以分母


  }
}
