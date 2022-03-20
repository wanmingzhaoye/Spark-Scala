package com.lzx.streaming

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import java.util
import java.util.concurrent.Future
import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer

object MockData {
  def main(args: Array[String]): Unit = {
    val prop: Properties = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop3-01:9092")
    //开启幂等性 ACKS 必须为-1 ,如果开始幂等性  retries、acks、max.in.flight.requests.per.connection 可以不配
    // prop.put(ProducerConfig.ACKS_CONFIG,"1")
    prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    //main线程--Producer-->拦截器-->序列化-->分区器-->acc共享变量-->thread线程
    //多个拦截器 用java的ArrayList
    val ls: util.ArrayList[String] = new util.ArrayList[String]()
    ls.add("com.lzx.streaming.kafkaInterceptor")
    ls.add("com.lzx.streaming.kafkaInterceptorAcc")
    prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,ls)
    //序列化
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //分区器
    prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.lzx.streaming.Mypartitioner")
    val pd: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    val topic: String = "atgugui"
    while (true) {
      mackdata().foreach(
        data => {
          val reader: ProducerRecord[String, String] = new ProducerRecord(topic, data)
          // 异步发送
          //Callback 回调函数
          pd.send(reader,new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if(exception==null) println(s"${metadata.partition()} ${metadata.offset()} 。。。")
            }
          })
          //同步发送 pd.send(reader).get()
        }
      )

      Thread.sleep(1000)
    }

  }

  def mackdata() = {
    val buffer: ListBuffer[String] = ListBuffer[String]()
    val area: ListBuffer[String] = ListBuffer[String]("华南", "华中", "华北")
    val city: ListBuffer[String] = ListBuffer[String]("上海", "北京", "深圳")

    for (i <- 0 to 30) {
      var are = area(new Random().nextInt(3))
      var ct = city(new Random().nextInt(3))
      val userid: Int = new Random().nextInt(6)
      val adid: Int = new Random().nextInt(6)
      buffer.append(s"${System.currentTimeMillis()} ${are} ${ct} ${userid} ${userid}")

    }

    buffer
  }
}
