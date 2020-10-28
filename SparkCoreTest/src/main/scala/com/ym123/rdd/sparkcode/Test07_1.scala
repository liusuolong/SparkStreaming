package com.ym123.rdd.sparkcode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-28 16:45
 */
object Test07_1 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    //将数据放入样例类中
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

    //定义需要统计的页面
    val ids = List(1, 2, 3, 4, 5, 6, 7)
    //准备过滤数据
    val idsToIds: List[(Int, Int)] = ids.zip(ids.tail)
    /*
    (1,2)
    (2,3)
    (3,4)
    (4,5)
    (5,6)
    (6,7)
     */

    val idsZip: List[String] = idsToIds.map {
      case (id1, id2) => {
        id1 + "--" + id2
      }
    }
    /*
    1-2
    2-3
    3-4
    4-5
    5-6
    6-7
     */
    //计算分母
    //init去掉最后一个
    // 过滤出包含page_id
    val denominator: Map[Long, Long] = actionRDD.filter(action => ids.init.contains(action.page_id))
      .map(action => (action.page_id, 1L))
      .reduceByKey(_ + _)
      .collect()
      .toMap
    //denominator.foreach(println)
    /**
     * (5,3563)
     * (1,3640)
     * (6,3593)
     * (2,3559)
     * (3,3672)
     * (4,3602)
     */

    //计算分子
    //按照session_id分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

    //sessionRDD.foreach(println)

    //对不同的session_id的v进行操作
    val element: RDD[(String, List[String])] = sessionRDD.mapValues(
      datas => {
        //按时间排序

        val sortWithTime: List[UserVisitAction] = datas.toList.sortWith(
          (curr, next) => {
            curr.action_time < next.action_time
          }
        )
        //获取pageId

        val pageIds: List[Long] = sortWithTime.map(_.page_id)

        //形成单跳元组
        // List((,首页) (,详情)....)

        val pageToPageList: List[(Long, Long)] = pageIds.zip(pageIds.tail)

        //变换结构
        // (,首页) (,详情)....=>(...--首页) (...--详情)

        val pageAndPageList: List[String] = pageToPageList.map {
          case (page1, page2) => {
            page1 + "--" + page2
          }
        }
        pageAndPageList.filter(data => idsZip.contains(data))
      }
    )


    //聚合(page1-page2,sum)
    val pageMapRDD: RDD[(String, Int)] = element.map(_._2).flatMap(list => list).map((_, 1)).reduceByKey(_ + _)
    //pageMapRDD.collect().foreach(println)

    /*
    计算
    单跳转换率 = (首页-详情,个数)/(首页,个数)
     */
    pageMapRDD.foreach {
      case (page, sum) => {
        val pageIds: Array[String] = page.split("--")
        //分母
        val pageSum: Long = denominator.getOrElse(pageIds(0).toLong, 1L)
        println(page + "=" + sum.toDouble / pageSum)
      }
    }

    //4.关闭连接
    sc.stop()
  }
}
