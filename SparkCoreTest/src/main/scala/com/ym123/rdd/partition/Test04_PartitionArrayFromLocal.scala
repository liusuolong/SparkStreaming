package com.ym123.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-22 19:49
 */
object Test04_PartitionArrayFromLocal {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //指定3个分区
    val rdd: RDD[String] = sc.textFile("input/2.txt",3)

    rdd.saveAsTextFile("output4")
    //4.关闭连接
    sc.stop()
  }
}