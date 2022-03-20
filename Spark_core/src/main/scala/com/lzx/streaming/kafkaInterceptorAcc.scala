package com.lzx.streaming

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

import java.util

class kafkaInterceptorAcc extends ProducerInterceptor[String,String]{
  private var success: Int = 0
  
  private var error: Int = 0
  override def configure(configs: util.Map[String, _]): Unit = {

  }
  
  //处理每条数据 (onSend,onAcknowledgement循环调用，直到处理完这一批数据)
  override def onSend(record: ProducerRecord[String, String]): ProducerRecord[String, String] = {
    record
  }
  //每条数据处理结果metadata(成功、失败)
  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {
    if (metadata==null) success+=1 else  error+=1
  }
  //关闭,（所有数据都处理完了）
  override def close(): Unit = {
    println(s"success+${success} error+${error}")
  }
  //配置

}
