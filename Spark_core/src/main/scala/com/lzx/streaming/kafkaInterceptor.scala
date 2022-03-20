package com.lzx.streaming

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

import java.util

class kafkaInterceptor extends ProducerInterceptor[String,String]{
  override def configure(configs: util.Map[String, _]): Unit = {

  }
  
  //处理每条数据 (onSend,onAcknowledgement循环调用，直到处理完这一批数据)
  override def onSend(record: ProducerRecord[String, String]): ProducerRecord[String, String] = {
    new ProducerRecord(record.topic(),record.partition(),record.timestamp(),record.key(),record.value()+"Inter")
  }
  //每条数据处理结果metadata(成功、失败)
  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {

  }
  //关闭,（producer.close才执行）
  override def close(): Unit = {

  }
  //配置

}
