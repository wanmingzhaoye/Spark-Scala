package com.lzx.flume

import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor
import org.apache.flume.interceptor.Interceptor.Builder

import java.util

class MyInterceptor extends Interceptor{

  private var lsev: util.List[Event] = util.List[Event]

  override def initialize(): Unit ={
    lsev= util.ArrayList[Event]
  }

  //单个event拦截
  override def intercept(event: Event): Event = {
    val headers: util.Map[String, String] = event.getHeaders
    val body: Array[Byte] = event.getBody
    val str: String = new String(body)
    if (str.contains("atguigu")) {
      headers.put("type","atguigu")

    }else {
      headers.put("type","other")
    }
    event.setHeaders(headers)
    event
  }

  /*
  # 选择器
  a1.sources.r1.selector.type = multiplexing
  a1.sources.r1.selector.header = type
  # 与自定义拦截器中设置的头信息对应
    a1.sources.r1.selector.mapping.atguigu = c1
    a1.sources.r1.selector.mapping.other = c2
    根据
    */
  //按批量拦截
  override def intercept(list: util.List[Event]): util.List[Event] = {
    list.clear();
    list.forEach(
     event =>{
       lsev.add(intercept(event))
     }
    )
    list
  }

  override def close(): Unit = {

  }
}

class Configuration extends Builder {
  override def build(): Interceptor = {
     new MyInterceptor
  }

  //可获取配置信息
  override def configure(context: Context): Unit = {

  }
}