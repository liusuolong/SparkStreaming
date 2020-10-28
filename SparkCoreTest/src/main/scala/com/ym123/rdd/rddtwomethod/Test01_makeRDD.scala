package com.ym123.rdd.rddtwomethod

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 集合中创建RDD
 * parallelize
 * makeRDD
 *
 * @author ymstart
 * @create 2020-09-22 17:53
 */
object Test01_makeRDD {
  def main(args: Array[String]): Unit = {
    //1.配置信息
    val conf:SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    //2.提交信息
    val sc:SparkContext = new SparkContext(conf)

    //3.使用parallelize()创建rdd
    val rdd:RDD[Int]=sc.parallelize(Array(1,2,3,4,5))

    //rdd.collect().foreach(println)

    //4.使用makeRDD
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5))
    rdd1.collect().foreach(println)

    sc.stop()
  }
}
