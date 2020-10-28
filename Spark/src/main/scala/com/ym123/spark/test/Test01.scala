package com.ym123.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-10-25 17:18
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("")

    //4.关闭连接
    sc.stop()
  }
}
