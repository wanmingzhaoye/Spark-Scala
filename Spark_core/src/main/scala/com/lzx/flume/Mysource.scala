package com.lzx.flume

import org.apache.flume.{Context, Event, PollableSource}
import org.apache.flume.conf.Configurable
import org.apache.flume.event.{JSONEvent, SimpleEvent}
import org.apache.flume.source.AbstractSource

import java.util

class Mysource extends AbstractSource with  Configurable with  PollableSource{
  //用来连接外部数据源
  override def start(): Unit = super.start()

  //获取配置 可以设置默认值
  override def configure(context: Context): Unit = {

    val hd: String = context.getString("hd","aa")
    val gg: String = context.getString("atguigu","bb")
  }

  //将获取的数据转换为event
  override def process(): PollableSource.Status = {
    val event: SimpleEvent = new SimpleEvent()
    val map: util.HashMap[String, String] = new util.HashMap()
    try {
      event.setBody("从外部数据源获取的数据".getBytes())
      map.put("a", "1")
      event.setHeaders(map)
      //数据到推送到channel处理器通过事务进行处理
      getChannelProcessor.processEvent(event)
       PollableSource.Status.READY
    } catch {
          //退避算法
      case _=> PollableSource.Status.BACKOFF
    }

  }

  //每次退避底数 毫秒
  override def getBackOffSleepIncrement: Long = {
    100L
  }

  override def getMaxBackOffSleepInterval: Long = 60000L

  override def stop(): Unit = super.stop()


}
