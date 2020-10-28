package com.ym123.rdd.sparkcode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayOps, ListBuffer}

/**
 * @author ymstart
 * @create 2020-09-28 8:26
 */
object Test04_3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val mapRDD: RDD[UserVisitAction] = rdd.map {
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
    val flatmapRDD: RDD[(String, CategoryCountInfo)] = mapRDD.flatMap {
      case action: UserVisitAction => {
        if (action.click_category_id != -1) {
          List((action.click_category_id.toString, CategoryCountInfo(action.click_category_id.toString, 1, 0, 0)))
        } else if (action.order_category_ids != "null") {
          val list: ListBuffer[(String, CategoryCountInfo)] = new ListBuffer[(String, CategoryCountInfo)]
          val ids: ArrayOps.ofRef[String] = action.order_category_ids.split(",")
          for (id <- ids) {
            list.append((id, CategoryCountInfo(id, 0, 1, 0)))
          }
          list
        } else if (action.pay_category_ids != "null") {

          val list: ListBuffer[(String, CategoryCountInfo)] = new ListBuffer[(String, CategoryCountInfo)]
          val ids: ArrayOps.ofRef[String] = action.pay_category_ids.split(",")
          for (id <- ids) {
            list.append((id, CategoryCountInfo(id, 0, 0, 1)))
          }
          list
        } else {
          Nil
        }
      }
    }
    val reduceRDD: RDD[CategoryCountInfo] = flatmapRDD.reduceByKey(
      (info1, info2) => {
        info1.clickCount += info2.clickCount
        info1.orderCount += info2.orderCount
        info1.payCount += info2.payCount
        info1
      }
    ).map(_._2)

    reduceRDD.sortBy(info=>(info.clickCount,info.orderCount,info.payCount)).take(10).foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
