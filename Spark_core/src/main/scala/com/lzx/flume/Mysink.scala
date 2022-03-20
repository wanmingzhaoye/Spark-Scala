package com.lzx.flume

import org.apache.flume.{Channel, Context, Event, Sink, Transaction}
import org.apache.flume.conf.Configurable
import org.apache.flume.sink.AbstractSink
import org.slf4j.{Logger, LoggerFactory}

object Mysink extends AbstractSink with Configurable {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def configure(context: Context): Unit = {}

  override def start(): Unit = super.start()

  override def stop(): Unit = super.stop()

  override def process(): Sink.Status = {
    val channel: Channel = getChannel
    val transaction: Transaction = channel.getTransaction
    transaction.begin()
    var event:Event=null
    var bool: Boolean = true
    try {
      while (bool) {
        event = channel.take()
        if (event != null) {
          bool = false
          //
          //如果要写入外部系统，应该在提交事务之前写入外部系统
          //
          transaction.commit();
        }
      }
    } catch {
      case _ => {
        logger.info(s"error:${_}")
        transaction.rollback()
      }
    }
    Sink.Status.READY
  }
}
