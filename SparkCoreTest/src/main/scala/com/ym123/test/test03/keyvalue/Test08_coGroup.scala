package com.ym123.test.test03.keyvalue

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
 * @author ymstart
 * @create 2020-09-25 9:08
 */
object Test08_coGroup {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(("hello",2),("hi",3),("tom",1),("hello",1)))
    val rdd2 = sc.makeRDD(List(("hello",2),("tom",1),("hello",1)))
    rdd1.cogroup(rdd2).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
