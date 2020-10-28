package com.ym123.rdd2.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-10-26 21:20
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/1.txt")

    println(rdd.flatMap(_.split(" ").groupBy(word => word).map(tuple => (tuple._1, tuple._2.size))).groupByKey().collect().mkString(","))

    //4.关闭连接
    sc.stop()
  }
}
