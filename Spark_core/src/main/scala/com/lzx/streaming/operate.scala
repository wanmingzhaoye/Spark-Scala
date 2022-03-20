package com.lzx.streaming




import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import scala.collection.mutable
object operate {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val sc: StreamingContext = new StreamingContext(conf, Seconds(3))
    sc.checkpoint("///")
    val kafkapara: Map[String, Object] = Map[String, Object](
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop3-01,hadoop3-02,hadoop3-03"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.GROUP_ID_CONFIG, "atguigu"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    )
    val fromOffsets: mutable.Map[TopicPartition, Long] = mutable.Map.empty[TopicPartition, Long]

    fromOffsets.put(new TopicPartition("atgugui",1),20L)

    val kdsm: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguiguNew"), kafkapara, fromOffsets))

    var ranges: Array[OffsetRange] =Array[OffsetRange]

    kdsm.transform(rdd=>{
      ranges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val Dmp: DStream[(String, Int)] = kdsm.map(data => (data.value(), 1))

    //必须checkpoint
    val upstate: DStream[(String, Int)] = Dmp.updateStateByKey(
      (va: Seq[Int], buffe: Option[Int]) => {
        Option(buffe.getOrElse(0) + va.sum)
      })
    val windows: DStream[(String, Int)] = upstate.window(Seconds(5), Seconds(3))
    val value: DStream[Long] = upstate.countByWindow(Seconds(5), Seconds(3))
    windows.reduceByWindow(
      (d1,d2)=>{(d1._1,d1._2+d2._2)}
      ,Seconds(5), Seconds(3))

    //加上新加的
    //减去超过的
    windows.reduceByKeyAndWindow(
      (x,y) => {x+y},
      (x,y) => {x-y},
      Seconds(5), Seconds(3)
    )

    windows.foreachRDD(
      (RDD,time)=>{
        RDD.foreach(k=>{println(s"k:${k._1};v:${k._2};;${time}")})
      }
    )
    ranges.foreach(offset=>
    {
      offset.partition
      offset.topic
      offset.fromOffset
      offset.untilOffset
    })


    sc.start()
    val thread: Thread = new Thread(myThead(sc))
    thread.start()
    sc.awaitTermination()

  }

}

case class myThead(sc:StreamingContext) extends Runnable{

  def isToStop():Boolean={true}
  override def run(): Unit = {
    while (true){
      try {
        Thread.sleep(10000)
      } catch {
        case e  => e.printStackTrace()
      }
    }

    if(!isToStop){
      if(StreamingContextState.ACTIVE==sc.getState()){
        sc.stop(true,true)
      }
    }

  }
}
