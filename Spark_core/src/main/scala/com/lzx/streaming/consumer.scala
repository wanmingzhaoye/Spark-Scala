package com.lzx.streaming


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}

import java.time.Duration
import java.util
import java.util.Properties

object consumer {
  def main(args: Array[String]): Unit = {

    val prop: Properties = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop3-01:9092")
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true")
    prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000")
    prop.put(ConsumerConfig.GROUP_ID_CONFIG,"datas")
    //重置消费者的offset  earliest最早offset开始消费 latest最新的offset开始
    //两种情况消费者更换 或者offset过期
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
    val consumer1: KafkaConsumer[String,String] = new KafkaConsumer[String,String](prop)
    consumer1.subscribe(util.Arrays.asList("atgugui"))
    while (true)
    {
      val records: ConsumerRecords[String, String] = consumer1.poll(Duration.ofMillis(100))
      records.forEach(
        record=>{
          println(record.toString)
          println("t:"+record.topic()+"p:"+record.partition()+"o:"+record.offset()+"--"+record.key()+":"+record.value())
        }
      )
    }

  }

}
