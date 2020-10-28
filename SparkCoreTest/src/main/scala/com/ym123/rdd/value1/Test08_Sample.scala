package com.ym123.rdd.value1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 23:25
 */
object Test08_Sample {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,11,22,44,33))

    //抽样不放回 伯努利
    rdd.sample(false,0.5,10).collect().foreach(println)
    println("=================================")
    //抽样放回 泊松
    rdd.sample(true,1,5).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
