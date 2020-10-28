package com.ym123.rdd.value1

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
 * @author ymstart
 * @create 2020-09-24 20:48
 */
object Test04_Flatmap {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(2,5),List(2,4)))
    rdd.flatMap(list =>list).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
