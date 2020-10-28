package com.ym123.test.test02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**11、distinct()去重源码解析（自己写算子实现去重）
 * @author ymstart
 * @create 2020-09-24 9:15
 */
object Test11 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,1,2,1,4))
    val distinctRDD: RDD[Int] = rdd.distinct()
    distinctRDD.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
