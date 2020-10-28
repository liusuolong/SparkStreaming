package com.ym123.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-25 9:38
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))
    println(rdd.reduce(_ + _))
    println("=========================")
    //以数组的形式返回
    rdd.collect().foreach(println)
    println("=========================")
    //返回元素个数
    println(rdd.count())
    println("=========================")
    //取RDD的第一个元素
    println(rdd.first())
    println("=========================")
    //取出前n个元素
    println(rdd.take(2).mkString(" "))
    //排序后取前N个
    println(rdd.takeOrdered(3).mkString(" "))

    //4.关闭连接
    sc.stop()
  }
}
