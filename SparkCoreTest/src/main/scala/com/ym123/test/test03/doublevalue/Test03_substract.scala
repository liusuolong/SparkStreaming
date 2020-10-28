package com.ym123.test.test03.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-25 8:51
 */
object Test03_substract {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,4,5,5))
    val rdd2: RDD[Int] = sc.makeRDD(List(3,4,4,5,5))
    rdd1.subtract(rdd2).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
