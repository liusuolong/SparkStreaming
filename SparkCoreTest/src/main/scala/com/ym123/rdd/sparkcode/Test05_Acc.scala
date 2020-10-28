package com.ym123.rdd.sparkcode

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext, rdd}

import scala.collection.{immutable, mutable}

/**
 * @author ymstart
 * @create 2020-09-27 19:54
 */
object Test05_Acc {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1  读取数据
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    //3.2 将每一行数据 封装到样例类中
    val actionRDD = rdd.map {
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
    //3.3创建累加器
    val acc: CategoryCountAccumulator = new CategoryCountAccumulator()
    //3.4 注册累加器
    sc.register(acc, "sum")

    //3.5 添加数据
    actionRDD.foreach(action => acc.add(action))

    //3.6 获取累加器的值
    //[(鞋,click)，10]
    val accMap: mutable.Map[(String, String), Long] = acc.value

    //3.7按品类分组(id,map((id,"click"),值) (id,"click"),值) (id,"click"),值))
   val groupRDD: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)

    //3.8 变换结构
    val infos: immutable.Iterable[(String, Long, Long, Long)] = groupRDD.map {
      case (id, map) => {
        val click = map.getOrElse((id, "click"), 0L)
        val order = map.getOrElse((id, "order"), 0L)
        val pay = map.getOrElse((id, "pay"), 0L)
        (id, click, order, pay)
      }
    }
    //3.9 排序
    infos.toList.sortWith(
      (left,right) =>{
        if(left._2 > right._2){
          true
        }else if (left._2 == right._2){
          if(left._3 > right._3){
            true
          }else if(left._3 == right._3){
            left._4 > right._4
          }else{
            false
          }
        }else{
          false
        }
      }
    ).take(10).foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

/**
 * UserVisitAction 样例类
 * mutable.Map[(String,String),Long]  [(鞋，click),2]
 */
class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {

  var map = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    new CategoryCountAccumulator
  }

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    //点击
    if (v.click_category_id != -1) {
      //[(String, String), Long]
      val key = (v.click_category_id.toString, "click")
      //历史+本次
      map(key) = map.getOrElse(key, 0L) + 1L
      //下单
    } else if (v.order_category_ids != "null") {
      //先进行切分
      val ids: Array[String] = v.order_category_ids.split(",")
      for (id <- ids) {
        val key = (id, "order")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
      //支付
    } else if (v.pay_category_ids != "null") {
      val ids: Array[String] = v.pay_category_ids.split(",")
      for (id <- ids) {
        val key = (id, "pay")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    } else {
      Nil
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    //循环取值
    other.value.foreach {
      case (key, count) => {
        map(key) = map.getOrElse(key, 0L) + count
      }
    }
  }
  override def value: mutable.Map[(String, String), Long] = map
}