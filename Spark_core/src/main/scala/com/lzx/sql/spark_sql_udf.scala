package com.lzx.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object spark_sql_udf {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //type DataFrame = Dataset[Row]
    import spark.implicits._
    //文件屬性按字典顺序排列
    val df: DataFrame = spark.read.json("Spark_core/src/main/datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName",
      (name:String)=>{
        "Name:"+name
      }
    )

    spark.sql("select age, prefixName(username) from user").show()


    spark.close()

  }

  case class User(id:Int,name:String,age:Int)
}

