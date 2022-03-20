package com.lzx

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapPartions {
  //外部文件源
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Test")
    val context: SparkContext = new SparkContext(conf)
    val ints: Array[Int] = Array(1, 2, 3, 5, 9, 4, 10, 20)
    //输入一个迭代器 返回一个迭代器
    //一次输入一个分区的数据进行计算 高性能 数据可以多变少 内存消耗大
    val rdd: RDD[Int] = context.makeRDD(ints, 3)
    rdd.mapPartitions(
      iter => List(iter.max).iterator
    ).foreach(println)
    //mapPartitionsWithIndex 一个分区数据和改分区的索引
    rdd.mapPartitionsWithIndex(
      (iter,indx)=>
      {
        if (indx==1) return iter else Nil.iterator
      }
    )

    context.stop()
  }
//配置log4j文件 在 resouces和classes里
}
