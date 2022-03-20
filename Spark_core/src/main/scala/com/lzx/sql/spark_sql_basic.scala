package com.lzx.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object spark_sql_basic {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //type DataFrame = Dataset[Row]
    import spark.implicits._
    //val df: DataFrame = spark.read.json("Spark_core/src/main/datas/user.json")
    spark.sql("select * from json.`Spark_core/src/main/datas/user.json`")
   // df.show()

    //df.createOrReplaceTempView("user")
    //spark.sql("select username,count(age) from user group by username").show()

    //DSL.
    //df.select("username","age")

    //f.select('age)
    //type DataFrame = Dataset[Row]
//    val set: Seq[Int] = Seq(1, 2, 3, 4)
//    val ds: Dataset[Int] = set.toDS()
//    ds.show()
//    ds.select('age)
    //SparkSession 封装了sparkContext
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "张三", 20), (2, "李四", 10), (3, "王五", 22)))
    val df: DataFrame = rdd.toDF("id","name","age")
    val ds: Dataset[User] = df.as[User]
    df.show()
    ds.show()

    val rdd1: RDD[Row] = df.rdd
    val rddtods: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    rddtods.show()
    val rdd2: RDD[User] = rddtods.rdd
    rddtods.write.mode(SaveMode.Overwrite).json("Spark_core/src/main/output")
    rddtods.write.format("scv").option("se",";")
    spark.close()

  }

  case class User(id:Int,name:String,age:Int)
}

