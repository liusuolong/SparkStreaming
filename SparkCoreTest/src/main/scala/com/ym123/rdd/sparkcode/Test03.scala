package com.ym123.rdd.sparkcode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author ymstart
 * @create 2020-09-27 14:04
 */
object Test03 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 读取数据
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    //3.2 将每一行数据封装到样例类中
    val actionRDD: RDD[UserVisitAction] = rdd.map(
      line => {
        val datas: Array[String] = line.split("_")
        //将数据放入样例类中
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
          datas(12).toLong,
        )
      }
    )
    //3.3 将订单ids 支付ids 解析出来
    val infoRDD: RDD[CategoryCountInfo] = actionRDD.flatMap {
      //匹配样例类
      case act: UserVisitAction => {
        //点击信息处理
        if (act.click_category_id != -1) {
          //将对象放入集合中
          List(CategoryCountInfo(act.click_category_id.toString, 1, 0, 0))
          //订单信息处理
        } else if (act.order_category_ids != "null") {

          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]

          //有多条订单信息,按,切分
          val ids = act.order_category_ids.split(",")

          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 1, 0))
          }
          list

          //支付信息
        } else if (act.pay_category_ids != "null") {
          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
          //有多条支付信息 按,切分
          val ids = act.pay_category_ids.split(",")
          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 0, 1))
          }
          list
        } else {
          Nil
        }
      }
    }
    //3.4 按品类进行分组
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(info => info.categoryId)

    //3.5 对数据进行聚合
    val mapRDD: RDD[CategoryCountInfo] = groupRDD.mapValues(
      //reduce(_+_) =>  reduce(info1,info2)=>{info1+info2}
      datas => datas.reduce(
        //左边和右边的数据
        (info1, info2) => {
          info1.clickCount += info2.clickCount
          info1.orderCount += info2.orderCount
          info1.payCount += info2.payCount
          info1
        }
      )
    ).map(_._2)
    //3.6 排序取top10
    val sortRDD: Array[CategoryCountInfo] = mapRDD.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false).take(10)
    sortRDD.take(10).foreach(println)

    /**
     * CategoryCountInfo(15,6120,1672,1259)
     * CategoryCountInfo(2,6119,1767,1196)
     * CategoryCountInfo(20,6098,1776,1244)
     * CategoryCountInfo(12,6095,1740,1218)
     * CategoryCountInfo(11,6093,1781,1202)
     * CategoryCountInfo(17,6079,1752,1231)
     * CategoryCountInfo(7,6074,1796,1252)
     * CategoryCountInfo(9,6045,1736,1230)
     * CategoryCountInfo(19,6044,1722,1158)
     * CategoryCountInfo(13,6036,1781,1161)
     */

    //4.关闭连接
    sc.stop()
  }
}

//用户访问动作表
case class UserVisitAction(date: String, //用户点击行为的日期
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
case class CategoryCountInfo(var categoryId: String, //品类id
                             var clickCount: Long, //点击次数
                             var orderCount: Long, //订单次数
                             var payCount: Long) //支付次数

