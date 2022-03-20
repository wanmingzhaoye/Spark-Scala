package com.lzx.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Category")

    val context: SparkContext = new SparkContext(conf)

    val tf: RDD[String] = context.textFile("Spark_core/src/main/datas/user_visit_action.txt")
    tf.cache()

   //2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
   val fmrdd: RDD[(String, (Int, Int, Int))] = tf.flatMap(
     datas => {
       val data: Array[String] = datas.split("_")
       if (data(6) != "-1") List((data(6), (1, 0, 0)))
       else if (data(8) != "null") {
         val listcategory_id: Array[String] = data(8).split(",")
         listcategory_id.map((_, (0, 1, 0)))
       }
       else if (data(10) != "null") {
         val listcategory_id: Array[String] = data(10).split(",")
         listcategory_id.map((_, (0, 0, 1)))
       } else {
         Nil
       }
     }
   )
    val analysisRDD: RDD[(String, (Int, Int, Int))] = fmrdd.reduceByKey(
      (t1, t2) =>
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    )
    analysisRDD.sortBy(_._2,false).take(10).foreach(println)

    println("=======================")
    //最快 只要是wordcount都能用累加器实现
    val acc = new HotCategoryAccumulator
    context.register(acc,"HotCategoryAccumulator")

    tf.foreach(
      datas=>{
        val data: Array[String] = datas.split("_")
        if (data(6) != "-1")  acc.add(data(6),"click")
        else if (data(8) != "null") {
          val listcategory_id: Array[String] = data(8).split(",")
          listcategory_id.foreach(acc.add(_,"order"))
        }
        else if (data(10) != "null") {
          val listcategory_id: Array[String] = data(10).split(",")
          listcategory_id.foreach(acc.add(_,"pay"))
        }
      }
    )
    val categories: mutable.Iterable[HotCategory] = acc.value.map(_._2)
    val tuples: mutable.Iterable[(String, (Int, Int, Int))] = categories.map(data => (data.cid, (data.click, data.order, data.pay)))
    tuples.toList.sortBy(_._2).reverse.take(10).foreach(println)

    println("=========================")
    //clickdata
    val clickdata: RDD[String] = tf.filter {
      case (data) =>
        val datas: Array[String] = data.split("_")
        datas(6) != "-1"
    }
    //(click_category_id,1)
    val click: RDD[(String, Int)] = clickdata.map {
      case (data) => {
        val datas: Array[String] = data.split("_")
        (datas(6), 1)
      }
    }
    //(click_category_id,clicknum)
    val clicknum: RDD[(String, Int)] = click.reduceByKey(_ + _)

    //order
    val orderdata: RDD[String] = tf.filter (
      data =>{
        val datas: Array[String] = data.split("_")
        datas(8) != "null"
           }
    )

    //(click_category_id,1)
    val order: RDD[(String, Int)] = orderdata.flatMap(
      data =>{
        val datas: Array[String] = data.split("_")
        val listcategory_id: Array[String] = datas(8).split(",")
        listcategory_id.map((_,1))
      }
    )
    //(click_category_id,ordernum)
    val ordernum: RDD[(String, Int)] = order.reduceByKey(_ + _)

    //paydata

    val paydata: RDD[String] = tf.filter {
      case (data) =>
        val datas: Array[String] = data.split("_")
        datas(10) != "null"
    }

    //(click_category_id,1)
    val pay: RDD[(String, Int)] = paydata.flatMap(
      data =>{
        val datas: Array[String] = data.split("_")
        val listcategory_id: Array[String] = datas(10).split(",")
        listcategory_id.map((_,1))
      }
    )
    //(click_category_id,ordernum)
    val paynum: RDD[(String, Int)] = pay.reduceByKey(_ + _)

    //(click_category_id ,(clicknum,paynum,ordernum))
    val id_cpoIter: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clicknum.cogroup(paynum, ordernum)
    val id_cpo: RDD[(String, (Int, Int, Int))] = id_cpoIter.mapValues(
      cpo => {
      (cpo._1.sum, cpo._2.sum, cpo._3.sum)
      }
    )
    id_cpo.sortBy(_._2,false).take(10).foreach(println)
    context.stop()



  }

  //用户访问动作表
//  case class UserVisitAction(
//                              date: String,//用户点击行为的日期
//                              user_id: Long,//用户的 ID
//                              session_id: String,//Session 的 ID
//                              page_id: Long,//某个页面的 ID
//                              action_time: String,//动作的时间点
//                              search_keyword: String,//用户搜索的关键词
//                              click_category_id: Long,//某一个商品品类的 ID
//                              click_product_id: Long,//某一个商品的 ID
//                              order_category_ids: String,//一次订单中所有品类的 ID 集合
//                              order_product_ids: String,//一次订单中所有商品的 ID 集合
//                              pay_category_ids: String,//一次支付中所有品类的 ID 集合
//                              pay_product_ids: String,//一次支付中所有商品的 ID 集合
//                              city_id: Long
//                            )//城市 id

  case class HotCategory(var cid:String,var click:Int,var order:Int,var pay:Int)

  class HotCategoryAccumulator extends AccumulatorV2 [(String,String),mutable.Map[String,HotCategory]]{

    private val htmp=mutable.Map[String,HotCategory]()

    override def isZero: Boolean = {
      htmp.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      htmp.clear()
    }

    override def add(v: (String, String)): Unit = {
      val hc: HotCategory = htmp.getOrElse(v._1, HotCategory(v._1,0, 0, 0))
      if (v._2=="click") hc.click+=1
      else if (v._2=="order") hc.order+=1
      else if (v._2=="pay") hc.pay+=1
      htmp.update(v._1,hc)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val value1: mutable.Map[String, HotCategory] = other.value
      val htmp1: mutable.Map[String, HotCategory] = htmp
      value1.foreach{
        case (cid,data)=>{
          val category: HotCategory = this.htmp.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.click+=data.click
          category.order+=data.order
          category.pay+=data.pay
          htmp1.update(cid,category)
        }
    }


    }

    override def value: mutable.Map[String, HotCategory] = {
      htmp
    }
  }
  }





