package com.ym123.spark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-22 15:17
 */
object Test01_PartitionDefault {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value:RDD[Int] = sc.makeRDD(Array(1,2,3,4))

    //4.关闭连接
    sc.stop()
  }
}
