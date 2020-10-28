package com.ym123.rdd.sparkcode

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author ymstart
 * @create 2020-09-28 8:43
 */
object Test05_Acc1 {
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
    //创建累加器
    val acc: myAccumulator = new myAccumulator()

    //注册
    sc.register(acc,"sum")

    //使用累加器 添加数据
    actionRDD.foreach(action=>acc.add(action))

    //获取累加器的值
    val valueRDD: mutable.Map[(String, String), Long] = acc.value

    //按品类分组
    val groupRDD: Map[String, mutable.Map[(String, String), Long]] = valueRDD.groupBy(_._1._1)

    //变换结构
    val mapRDD: immutable.Iterable[CategoryCountInfo] = groupRDD.map {
      case (id, map) => {
        val click = map.getOrElse((id, "click"), 0L)
        val order = map.getOrElse((id, "order"), 0L)
        val pay = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }

    //排序
    //方法一:
    val top10: List[CategoryCountInfo] = mapRDD.toList.sortBy(a => (a.clickCount,a.orderCount,a.payCount)).reverse.take(10)
    //方法二:
    /*val top10: List[CategoryCountInfo] = mapRDD.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10)*/

    //取出top10热门品类
    val ids: List[String] = top10.map(_.categoryId)
    //ids.foreach(println)

    //将ids变成广播变量
    val b_ids: Broadcast[List[String]] = sc.broadcast(ids)

    //过滤(保留top10 和点击数据)
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
          b_ids.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    )
    //filterRDD.collect().foreach(println)
    //对session点击数进行转换
    val idAndSessionToOneRDD: RDD[(String, Int)] = filterRDD.map(
      action => (action.click_category_id + "--" + action.session_id, 1)
    )
    /**
     * (19--d79508b4-66bf-4410-a5bb-f67a8831610f,1)
     * (9--d79508b4-66bf-4410-a5bb-f67a8831610f,1)
     * (15--d79508b4-66bf-4410-a5bb-f67a8831610f,1)
     * (9--d79508b4-66bf-4410-a5bb-f67a8831610f,1)
     */
    //idAndSessionToOneRDD.collect().foreach(println)


    val idAndSessionToSum: RDD[(String, Int)] = idAndSessionToOneRDD.reduceByKey(_+_)
    /**
     * (13-63a136fd-0316-4ece-85f3-fa83f1d7ba7f,2)
     * (11-14f6e8e3-dfd5-4e6b-afaa-6169fae3ebbd,1)
     * (2-ced3d352-f2f1-4c7a-9d48-01ff880b0c4a,2)
     * (9-a1d8f31f-9060-494d-b5cd-b00dcb193a72,3)
     */
    //idAndSessionToSum.collect().foreach(println)

    val id_SessionAndSum: RDD[(String, (String, Int))] = idAndSessionToSum.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("--")
        (keys(0), (keys(1), sum))
      }
    }
    /**
     * (11,(a37db20a-cfd0-4a70-8989-157a0d65a53c,1))
     * (19,(e1e577b0-0d3f-42d1-9176-dccfac18e05b,1))
     * (12,(80fd7bcb-ef2b-49a9-a634-63ea3daf6466,1))
     */
    //id_SessionAndSum.collect().foreach(println)

    val groupByKeyRDD: RDD[(String, Iterable[(String, Int)])] = id_SessionAndSum.groupByKey()

    /**
     * (7,CompactBuffer((4b7590f1-a009-43c5-9b1a-cff8db2746f1,1), (45e35ffa-f0e0-400e-a252-5605b4089625,5)
     * (9,CompactBuffer((9eb23767-55d9-43e3-a30e-0e9343e34d43,2), (26a04ea6-7799-473d-aba5-cba4efbc366b,2)
     */
    //groupByKeyRDD.collect().foreach(println)
    val mapValueRDD: RDD[(String, List[(String, Int)])] = groupByKeyRDD.mapValues {
      datas => {
        datas.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(10)
      }
    }
    mapValueRDD.foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
class myAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{

  var map = mutable.Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    new myAccumulator
  }

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != -1){

      val key = (v.click_category_id.toString,"click")
      map(key) = map.getOrElse(key,0L)+ 1L

    }else if(v.order_category_ids != "null"){
      val ids: Array[String] = v.order_category_ids.split(",")
      for (id <- ids) {
        val key = (id,"order")
        map(key) = map.getOrElse(key,0L) + 1L
      }

    }else if(v.pay_category_ids != "null"){
      val ids: Array[String] = v.pay_category_ids.split(",")
      for (id <- ids) {
        val key =(id,"pay")
        map(key) = map.getOrElse(key,0L) + 1L
      }
    }else{
      Nil
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach{
     case (word,count)=>{
        map(word) = map.getOrElse(word,0L)+ count
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}
