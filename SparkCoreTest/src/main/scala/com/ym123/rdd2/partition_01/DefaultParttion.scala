package com.ym123.rdd2.partition_01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-10-26 18:30
 */
object DefaultParttion {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4))

    rdd.saveAsTextFile("/output1/1.txt")

    //4.关闭连接
    sc.stop()
  }
}
