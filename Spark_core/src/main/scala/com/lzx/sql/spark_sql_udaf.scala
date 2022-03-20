package com.lzx.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object spark_sql_udaf {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //type DataFrame = Dataset[Row]
    //文件屬性按字典顺序排列
    val df: DataFrame = spark.read.json("Spark_core/src/main/datas/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("avgage",new MyUDAFun())
    //弱类型操作，一行数据没有类
    spark.sql("select avgage(age) from user").show()


    spark.close()

  }

  class MyUDAFun extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = StructType(
      Array(
        StructField("age",LongType)
      )
    )

    override def bufferSchema: StructType =  StructType(
      Array(
        StructField("age",LongType),
        StructField("count",LongType)
      )
    )

    //结果输出类型
    override def dataType: DataType = LongType
    //函数的稳定性 true稳定无随机值
    override def deterministic: Boolean = true

    //初始值
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer.update(0,0L)
//      buffer.update(1,0L)
      buffer(0)=0L
      buffer(1)=0L

    }
      //根据输入值修改缓冲数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0)=buffer.getLong(0)+input.getLong(0)
      buffer(1)=buffer.getLong(1)+1L
    }
    //合并(((1+2)+3)+...)
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))

    }

    //计算平均值 相当于group by 之后进行函数计算
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)/buffer.getLong(1)
    }
  }
}

