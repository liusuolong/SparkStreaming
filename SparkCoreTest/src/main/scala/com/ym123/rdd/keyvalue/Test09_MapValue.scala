package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 18:20
 */
object Test09_MapValue {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",2),("hi",1),("hello",3),("oh",2),("hi",1)),3)
    //按value排序
    val mapValueRDD: RDD[(String, String)] = rdd.mapValues(_+"- _ -")
    mapValueRDD.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
