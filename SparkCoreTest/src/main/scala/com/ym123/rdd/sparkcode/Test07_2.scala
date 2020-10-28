package com.ym123.rdd.sparkcode

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-28 20:07
 */
object Test07_2 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    //放入样例类中
    val actionRDD: RDD[UserVisitAction] = rdd.map {
      line => {
        val data: Array[String] = line.split("_")
        UserVisitAction(
          data(0),
          data(1).toLong,
          data(2),
          data(3).toLong,
          data(4),
          data(5),
          data(6).toLong,
          data(7).toLong,
          data(8),
          data(9),
          data(10),
          data(11),
          data(12).toLong
        )
      }
    }
    //数据准备
    val ids = List(1, 2, 3, 4, 5, 6, 7)
    val idsToIds: List[(Int, Int)] = ids.zip(ids.tail)
    val idsAndIdList: List[String] = idsToIds.map {
      case (id1, id2) => {
        id1 + "-" + id2
      }
    }


    //计算分母
    //init去掉最后一个
    //filter 过滤出包含page_id
    //map 结构变换
    //reduceByKey 按page_id相同的聚合
    //toMap Map[Long, Long] (long,long)
    val denominator: Map[Long, Long] = actionRDD
      .filter(action => ids.init.contains(action.page_id))
      .map(data => (data.page_id, 1L))
      .reduceByKey(_ + _)
      .collect()
      .toMap

    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    //分子计算
    //groupBy 按session分组
    //flatMap
    //sortBy  按时间排序
    //map 获取page_id
    //zip 将page与没有头的自身的进行拉链=>List((详情,下单),(下单,支付)...)
    //map 变换结构  List((详情,下单),(下单,支付)...) List() => List((详情-下单),(下单-支付)...)
    //filter  过滤出包含page-page的数据
    val element: RDD[(String, List[String])] = sessionRDD.mapValues {
      data => { //按时间分组
        val sortByTime: List[UserVisitAction] = data.toList.sortBy(_.action_time)
        val pageId: List[Long] = sortByTime.map(_.page_id)
        val pageToPage: List[(Long, Long)] = pageId.zip(pageId.tail)
        val page_page: List[String] = pageToPage.map {
          case (page1, page2) => {
            page1 + "-" + page2
          }
        }
        page_page.filter(data => idsAndIdList.contains(data))
      }
    }
    //聚合
    //map RDD[(String, List[String])] 取出List
    //flatMap 打散
    //map 结构变换 List() => (1-2,1)
    //reduceByKey 计数(1-2,1) =>  (1-2,55)
    val reduceRDD: RDD[(String, Int)] = element.map(_._2).flatMap(list=>list).map((_,1)).reduceByKey(_+_)

    //计算
    reduceRDD.foreach{
      //(1-2,55)
      case(page,sum)=>{
        val pages: Array[String] = page.split("-")
        val pageSum: Long = denominator.getOrElse(pages(0).toLong,1L)
        println("结果为:"+page(0) +"="+ sum.toDouble / pageSum)
      }
    }

    //4.关闭连接
    sc.stop()
  }
}
