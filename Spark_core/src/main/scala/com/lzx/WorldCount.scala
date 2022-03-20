package com.lzx

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object WorldCount {
  //外部文件源
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Test")
    val context: SparkContext = new SparkContext(conf)
    //默认读取项目根目录（targeth和main）这里存在子项目读取的是父项目的根目录
    //val text: RDD[String] = context.textFile("/D:/Test/test.txt", 3)
    //context.wholeTextFiles() (文件路径，内容) 可获得文件路径
    // 最小分区数我 ：定义的分区数与默认(系统最大)(分区)核数比较取小
    val text: RDD[String] = context.textFile("Spark_core/src/main/datas/test.txt", 3)
    val fp: RDD[String] = text.flatMap(_.split("\\s+"))
    val value: RDD[(String, Int)] = fp
      .map((_, 1))
      .reduceByKey(_+_)
      .sortBy(_._2)
    val tuples: Array[(String, Int)] = value.collect()
    tuples.foreach(println)
    fp.groupBy(word => word) //(scala,CompactBuffer(scala, scala, scala, scala))
      .map (x=>(x._1,x._2.size))
      .collect().foreach(println)

    context.stop()
  }
//配置log4j文件 在 resouces和classes里
}
