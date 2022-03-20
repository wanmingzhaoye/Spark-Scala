package com.lzx.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object spark_sql_hivetest1 {
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

    spark.sql("""select
                |		a.click_product_id,b.product_name,c.area,c.city_name
                |		from user_visit_action a
                |		join product_info b on click_product_id==b.product_id
                |		join city_info c on a.city_id==c.city_id where a.click_product_id>-1""".stripMargin)
      .createOrReplaceTempView("t1")

    spark.udf.register("Remark", functions.udaf(new Remake()) )
    spark.sql("""select
                |	area,product_name,count(*) clikcnt ,Remark(city_name) as ctRemark
                |	from
                |	t1 group by area, product_name""".stripMargin).createOrReplaceTempView("t2")

    spark.sql("""select
                |*,rank() over(partition by area order by clikcnt desc) as rank
                |from t2 """.stripMargin).createOrReplaceTempView("t3")

    //false 不限制显示行数
     spark.sql(
          """select
                |	*
                |from t3 where rank<=3""".stripMargin).show(false)


    spark.close()
  }
  case class Buffe (var cnt:Long,var citycnt:mutable.Map[String,Long])

 class Remake extends Aggregator[String,Buffe,String]{
   override def zero: Buffe = {
     new Buffe (0L,mutable.Map[String,Long]())
   }

   override def reduce(b: Buffe, a: String): Buffe = {
     val citycnt: mutable.Map[String, Long] = b.citycnt
     val cl: Long = citycnt.getOrElse(a,0L) + 1L
     citycnt.update(a,cl)
     Buffe(b.cnt+1L,citycnt)
   }

   override def merge(b1: Buffe, b2: Buffe): Buffe = {

     b1.cnt+=b2.cnt
     val map1: mutable.Map[String, Long] = b1.citycnt
     val map2: mutable.Map[String, Long] = b2.citycnt

     map2.foreach{
      case (city,cnt)=>{
       val newCnt= map1.getOrElse(city, 0L) + cnt
        map1.update(city,newCnt)
      }
    }
     b1.citycnt = map1
     b1
   }

   override def finish(reduction: Buffe): String = {
     val total: Long = reduction.cnt
     val citycnt: mutable.Map[String, Long] = reduction.citycnt

     val mkstring: ListBuffer[String] = ListBuffer[String]()
     val ctcntls: List[(String, Long)] = citycnt.toList.sortWith(
       (t, t1)
       => (t._2 > t1._2)
     ).take(2)

     val bool: Boolean = citycnt.size>2
     var rsum:Long=0L
     ctcntls.foreach{
       case(city,cnt) => {
         val r: Long = cnt * 100 / total
         mkstring.append(s"${city}:${r}%")
         rsum += r
       }
     }
     if (bool){
       mkstring.append(s"其他:${100-rsum}%")
     }

     mkstring.mkString(",")
   }

   override def bufferEncoder: Encoder[Buffe] = Encoders.product

   override def outputEncoder: Encoder[String] = Encoders.STRING
 }
}

