package com.lzx.framework.server

import com.lzx.framework.common.TWordCountServer
import com.lzx.framework.dao.WordCountDao
import com.lzx.framework.model.UserVisitAction
import org.apache.spark.rdd.RDD

class WordCountServer extends TWordCountServer{
  private val dao: WordCountDao = new WordCountDao
  def dataAnalysis()={
    val actionRDD= dao.getDatas("Spark_core/src/main/datas/user_visit_action.txt")
    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionDataRDD.cache()

    // TODO 对指定的页面连续跳转进行统计
    // 1-2,2-3,3-4,4-5,5-6,6-7
    val ids = List[Long](1,2,3,4,5,6,7)
    val okflowIds: List[(Long, Long)] = ids.zip(ids.tail)

    // TODO 计算分母
    val pageidToCountMap: Map[Long, Long] = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    // TODO 计算分子

    // 根据session进行分组 一个人代表一个会话窗口
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    // 分组后，根据访问时间进行排序（升序）
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

        // 【1，2，3，4】
        // 【1，2】，【2，3】，【3，4】
        // 【1-2，2-3，3-4】
        // Sliding : 滑窗
        // 【1，2，3，4】
        // 【2，3，4】
        // zip : 拉链
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

        // 将不合法的页面跳转进行过滤
        pageflowIds.filter(
          t => {
            okflowIds.contains(t)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
      }
    )
    val value: RDD[List[((Long, Long), Int)]] = mvRDD.map(_._2)
    // ((1,2),1)
    // List[((Long, Long), Int)])
    val flatRDD: RDD[((Long, Long), Int)] = value.flatMap(list=>list)
    // ((1,2),1) => ((1,2),sum)
    val dataRDD = flatRDD.reduceByKey(_+_)

    // TODO 计算单跳转换率
    // 分子除以分母
    dataRDD.foreach{
      case ( (pageid1, pageid2), sum ) => {
        val lon: Long = pageidToCountMap.getOrElse(pageid1, 0L)

        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:" + ( sum.toDouble/lon ))
      }
    }
  }
}
