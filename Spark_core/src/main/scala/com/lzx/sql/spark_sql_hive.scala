package com.lzx.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object spark_sql_hive {
  def main(args: Array[String]): Unit = {

    //有hive权限的用户 ，去hdfs ui看
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    //val conf: SparkConf = new SparkConf()

    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("sql")
      //hive在hdfs的储存位置，去hdfs ui看
      .config("spark.sql.warehouse.dir", "hdfs://hadoop3cluster:9820/user/hive/warehouse")

      .getOrCreate()
    spark.sql("use atguigu")
    spark.sql("show tables").show
    spark.sql("select * from product_info").show

    spark.close()

  }

}

