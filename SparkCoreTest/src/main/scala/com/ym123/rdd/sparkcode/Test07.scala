package com.ym123.rdd.sparkcode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-28 14:28
 */
object Test07 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //读入数据
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    //数据转换
    val actionRDD: RDD[UserVisitAction] = rdd.map {
      line => {
        val datas: Array[String] = line.split("_")
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
    }
    //定义要统计的页面
    val ids = List(1, 2, 3, 4, 5, 6, 7)

    val idsZip: List[String] = ids.zip(ids.tail).map {
      case (page1, page2) => {
        page1 + "-" + page2
      }
    }

    //计算分母
    //过滤出page_id
    val isMap: Map[Long, Long] = actionRDD.filter(action => ids.init.contains(action.page_id))
      //结构变换
      // (首页,1) (详情,1)...
      .map(action => (action.page_id, 1L))
      .reduceByKey(_ + _).collect().toMap


    //计算分子
    //根据session_id 分组
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

    val infoRDD: RDD[List[String]] = groupRDD.mapValues(
      datas => {
        //按时间排序
        val sortWithTime: List[UserVisitAction] = datas.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        //获取pageId
        val pageIdList: List[Long] = sortWithTime.map(_.page_id)
        //自己与不要头的自己拉链
        // (首页,详情) (详情,下单)...

        //形成单跳元组
        val pageToPageList: List[(Long, Long)] = pageIdList.zip(pageIdList.tail)

        //变换结构
        val pageAndPage: List[String] = pageToPageList.map {
          case (page1, page2) => {
            page1 + "-" + page2
          }
        }
        pageAndPage.filter(datas => idsZip.contains(datas))
      }
    ).map(_._2)

    //聚合统计
    /**
     * (3-4,62)
     * (5-6,52)
     * (6-7,69)
     * (4-5,66)
     * (1-2,55)
     * (2-3,71)
     */
    val pageMapRDD: RDD[(String, Int)] = infoRDD.flatMap(list => list)
                                                .map((_, 1))
                                                .reduceByKey(_ + _)

    //计算页面跳转率
    pageMapRDD.foreach {
      case (page, sum) => {
        val pageIds: Array[String] = page.split("-")
        //分母
        val pageSum: Long = isMap.getOrElse(pageIds(0).toLong, 1)
        println(page + "=" + sum.toDouble/pageSum)
      }
    }
    //4.关闭连接
    sc.stop()
  }
}
