package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 18:13
 */
object Test08_SortByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",2),("hi",1),("hello",3),("oh",2),("hi",1)))
    //按元组的二个元素 正序排
    val sortByRDD: RDD[(String, Int)] = rdd.sortBy(_._2,true)
    sortByRDD.collect().foreach(println)
    //按元组的二个元素 倒序排
    val sortByRDD1: RDD[(String, Int)] = rdd.sortBy(_._2,false)
    sortByRDD1.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
