package com.ym123.rdd.sparkcode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayOps, ListBuffer}

/**
 * @author ymstart
 * @create 2020-09-27 15:28
 */
object Test04 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 读取数据
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    //3.2 map
    val actionRDD: RDD[UserVisitAction] = rdd.map(
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
    )
    println("3333333333333333")

    //3.3 flatMap
    val infoRDD: RDD[(String, CategoryCountInfo1)] = actionRDD.flatMap {
      case act: UserVisitAction1 => {
        if (act.click_category_id != -1) {
          println("55555555555")
          //(k,v)
          List((act.click_category_id.toString,CategoryCountInfo1(act.click_category_id.toString,1,0,0)))


        } else if (act.order_category_ids != "null") {

          val list: ListBuffer[(String, CategoryCountInfo1)] = new ListBuffer[(String, CategoryCountInfo1)]
          val ids = act.order_category_ids.split(",")
          for (id <- ids) {
            //将数据放入集合
            list.append((id, CategoryCountInfo1(id, 0, 1, 0)))
          }
          println("4444444444444")
          list
        } else if (act.pay_category_ids != "null") {

          val list1: ListBuffer[(String, CategoryCountInfo1)] = new ListBuffer[(String, CategoryCountInfo1)]
          val ids1 = act.pay_category_ids.split(",")
          for (id <- ids1) {
            list1.append((id, CategoryCountInfo1(id, 0, 0, 1)))
          }
          println("666666666")
          list1
        } else {
          Nil
        }
      }
    }
    println("22222222222")

    //3.4 reducerByKey
    val mapRDD: RDD[CategoryCountInfo1] = infoRDD.reduceByKey(
      (info1, info2) => {
        info1.clickCount += info2.clickCount
        info1.orderCount += info2.orderCount
        info1.payCount += info2.payCount
        info1
      }
    ).map(_._2)
    println("111111111111111")
    //分组
    mapRDD.sortBy(
      info =>
        (info.clickCount,info.orderCount,info.payCount),false).take(10).foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

//用户访问动作表
case class UserVisitAction1(date: String, //用户点击行为的日期
                            user_id: Long, //用户的ID
                            session_id: String, //Session的ID
                            page_id: Long, //某个页面的ID
                            action_time: String, //动作的时间点
                            search_keyword: String, //用户搜索的关键词
                            click_category_id: Long, //某一个商品品类的ID
                            click_product_id: Long, //某一个商品的ID
                            order_category_ids: String, //一次订单中所有品类的ID集合
                            order_product_ids: String, //一次订单中所有商品的ID集合
                            pay_category_ids: String, //一次支付中所有品类的ID集合
                            pay_product_ids: String, //一次支付中所有商品的ID集合
                            city_id: Long) //城市 id
// 输出结果表
case class CategoryCountInfo1(var categoryId: String, //品类id
                              var clickCount: Long, //点击次数
                              var orderCount: Long, //订单次数
                              var payCount: Long) //支付次数
