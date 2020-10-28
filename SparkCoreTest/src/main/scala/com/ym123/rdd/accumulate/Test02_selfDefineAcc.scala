package com.ym123.rdd.accumulate

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author ymstart
 * @create 2020-09-27 10:29
 */
object Test02_selfDefineAcc {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"))

    //3.2创建累加器
    val accumulate: MyAccumulate = new MyAccumulate()

    //3.3注册累加器
    sc.register(accumulate,"WordCount")

    //3.4使用自定义累加器
    rdd.foreach(
      word => accumulate.add(word)
    )

    //3.5打印结果
    println(accumulate.value) //Map(Hello -> 5)

    //4.关闭连接
    sc.stop()
  }
}
//自定义累加器
class MyAccumulate extends AccumulatorV2[String,mutable.Map[String,Int]]{

  //定义输出数据集合
  var map = mutable.Map[String,Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    new MyAccumulate
  }

  override def reset(): Unit = map.clear()

  //添加数据
  override def add(v: String): Unit = {
    if(v.startsWith("H")){
      //如果是H 开头的就添加到map中
      map(v) = map.getOrElse(v,0) + 1
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    other.value.foreach{
      case (word,count) =>{
        map(word) = map.getOrElse(word,0) +count
      }
    }
  }

  //返回结果
  override def value: mutable.Map[String, Int] = map
}