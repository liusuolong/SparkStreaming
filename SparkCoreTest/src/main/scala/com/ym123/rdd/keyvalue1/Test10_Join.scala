package com.ym123.rdd.keyvalue1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-26 21:29
 */
object Test10_Join {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello", 2), ("hello",1),("hi", 2), ("yy", 2), ("hh", 2), ("hi", 1)), 1)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("hi", 2), ("yy", 2), ("hh", 2), ("hi", 1), ("hello", 2)), 1)

    //将分区中相同的K聚合到一个元组,如果只有一个分区中有的k则会被舍弃
    rdd.join(rdd1).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
