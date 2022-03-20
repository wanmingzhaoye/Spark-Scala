package com.lzx

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//内存创建数据源(并行)
object Parallelize {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc: SparkContext = new SparkContext(conf)


    //sc.parallelize()
    //makeRDD 调用了parallelize
    val ls: List[(String, Int)] = List(
      ("hadoop hadoop scala", 2),
      ("scala hadoop", 3),
      ("hello world", 2),
      ("hello scala", 1),
      ("hrro hello", 2),
      ("spark good", 1),
      ("spark hello scala", 3)
    )
    //并行度和分区数有关 设置并行度能设置输出分区文件的给个数 默认为系统核数
    val lsrdd: RDD[(String, Int)] = sc.makeRDD(ls,4)
    val fm: RDD[(String, Int)] = lsrdd.flatMap(ls => ls._1.split("\\s+").map((_, ls._2)))
    val rdbk: RDD[(String, Int)] = fm.reduceByKey(_+_)
    val sb: RDD[(String, Int)] = rdbk.sortBy(_._2,false)
    //目标文件夹要不存在
    sb.saveAsTextFile("Spark_core/src/main/output")
    sb.collect().take(3).foreach(println)



  }
}
