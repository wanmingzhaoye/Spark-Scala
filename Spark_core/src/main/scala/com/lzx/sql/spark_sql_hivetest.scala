package com.lzx.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object spark_sql_hivetest {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("sql")
      //hive在hdfs的储存位置，去hdfs ui看
      .config("spark.sql.warehouse.dir", "hdfs://hadoop3cluster:9820/user/hive/warehouse")
      .getOrCreate()

    spark.sql("use atguigu")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited
        |fields terminated by '\t'
            """.stripMargin)


    spark.sql(
      """
        |CREATE TABLE if not exists `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'Spark_core/src/main/datas/user_visit_action.txt' into table atguigu.user_visit_action
            """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'Spark_core/src/main/datas/product_info.txt' into table atguigu.product_info
            """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'Spark_core/src/main/datas/city_info.txt' into table atguigu.city_info
            """.stripMargin)

    spark.sql("select * from user_visit_action").show

    spark.close()

  }
}



