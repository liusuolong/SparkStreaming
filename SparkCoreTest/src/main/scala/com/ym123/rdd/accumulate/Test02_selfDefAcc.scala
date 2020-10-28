package com.ym123.rdd.accumulate

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author ymstart
 * @create 2020-09-27 22:58
 */
object Test02_selfDefAcc {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //数据
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),1)

    //创建累加器
    val acc: myAcc = new myAcc()

    //注册累加器
    sc.register(acc,"sum")

    //使用累加器

    //4.关闭连接
    sc.stop()
  }
}
class myAcc extends AccumulatorV2[String,mutable.Map[String,Int]]{

  var map = mutable.Map[String,Int]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    new myAcc
  }

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if(v.contains("a")){
      map(v) = map.getOrElse(v,0) +1
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    other.value.foreach{
      case(word,count) =>{
        map(word) = map.getOrElse(word,0) +count
      }
    }
  }

  override def value: mutable.Map[String, Int] = map
}