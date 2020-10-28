package com.ym123.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-25 11:46
 */
object Test04_countByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("hi",2),("tom",2),("hello",2)))
    println(rdd.countByKey())

    //4.关闭连接
    sc.stop()
  }
}
