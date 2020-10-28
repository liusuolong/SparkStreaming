package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**直接进行shuffle
 * @author ymstart
 * @create 2020-09-23 21:13
 */
object Test04_GroupByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1,"hello"),(1,"hi"),(2,"tom"),(2,"jerry"),(3,"二狗子")))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("hi",2),("tom",2),("jerry",1),("二狗子",1),("hello",1)))
    val group: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
    //groupByKeyRDD.collect().foreach(println)
    group.map(t =>(t._1,t._2.sum)).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
