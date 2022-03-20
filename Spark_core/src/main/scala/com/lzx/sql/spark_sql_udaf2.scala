package com.lzx.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object spark_sql_udaf2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //type DataFrame = Dataset[Row]
    //文件屬性按字典顺序排列
    val df: DataFrame = spark.read.json("Spark_core/src/main/datas/user.json")
    df.createOrReplaceTempView("user")
    //spark3.0 之前
    //spark.udf.register("avgage",functions.udaf(new MyUDAFun))

    val ds: Dataset[user] = df.as[user]
    val column: TypedColumn[user, Long] = new MyUDAFun().toColumn
    ds.select(column).show()

    
   // spark.sql("select avgage(age) from user").show()


    spark.close()

  }

  case class user(username: String,age:Long)

  case class buff(age: Long,count:Long)

  class MyUDAFun extends Aggregator[user,buff,Long] {
    override def zero: buff = buff(0L,0L)

    override def reduce(b: buff, a: user): buff = {
      buff(b.age+a.age,b.count+1)
    }

    override def merge(b1: buff, b2: buff): buff = {
      buff(b1.age+b2.age,b1.count+b2.count)
    }

    //计算结果
    override def finish(reduction: buff): Long = {
      reduction.age/reduction.count
    }

    //缓冲区数据编码
    override def bufferEncoder: Encoder[buff] =Encoders.product
    //输出数据编码
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}

