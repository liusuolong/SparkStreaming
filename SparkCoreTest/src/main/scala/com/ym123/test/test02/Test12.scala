package com.ym123.test.test02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 9:20
 */
object Test12 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    sc.textFile("input/1.txt").flatMap(_.split(" ")).map((_,1)).groupBy((_._1)).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
