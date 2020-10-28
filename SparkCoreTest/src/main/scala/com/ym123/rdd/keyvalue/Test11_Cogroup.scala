package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 18:35
 */
object Test11_Cogroup {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",2),("hi",1),("hello",3),("oh",2),("hi",1)))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("hello",2),("hi",1),("hello",3),("tom",2),("jerry",1)))
    //rdd 与 rdd1 按key相同放入同一个迭代器中
    rdd.cogroup(rdd1).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
