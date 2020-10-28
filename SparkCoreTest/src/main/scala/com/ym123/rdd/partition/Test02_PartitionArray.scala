package com.ym123.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** 设置分区数
 * @author ymstart
 * @create 2020-09-22 19:09
 */
object Test02_PartitionArray {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //设置4个分区
    val rdd:RDD[Int] = sc.makeRDD(Array(1,2,3,4),4)
    //rdd.saveAsTextFile("output1")

    val rdd1:RDD[Int] = sc.makeRDD(Array(1,2,3,4),2)
    rdd1.saveAsTextFile("output1-1")
    //4.关闭连接
    sc.stop()
  }
}
