package com.ym123.rdd.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**分布式只读变量
 * @author ymstart
 * @create 2020-09-27 11:55
 */
object Test01_Broadcast {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.采用集合的方式，实现rdd1和list的join
    val rdd: RDD[String] = sc.makeRDD(List("WARN:Class Not Find", "INFO:Class Not Find", "DEBUG:Class Not Find"), 4)
    val list: String = "WARN"

    //声明广播变量
    val warn: Broadcast[String] = sc.broadcast(list)

    val filterRDD: RDD[String] = rdd.filter(_.contains("WARN"))
    filterRDD.foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
