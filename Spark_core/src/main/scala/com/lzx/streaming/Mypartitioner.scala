package com.lzx.streaming

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

import java.util


class Mypartitioner extends Partitioner {
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    //返回分区号
     0
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
