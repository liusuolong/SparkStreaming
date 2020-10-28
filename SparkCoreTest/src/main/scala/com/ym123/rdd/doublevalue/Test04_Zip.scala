package com.ym123.rdd.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**拉链
 * 参数个数相同 分区数相同 -> 元组
 * @author ymstart
 * @create 2020-09-23 18:32
 */
object Test04_Zip {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3),3)
    val rdd2: RDD[Int] = sc.makeRDD(Array(4,5,6),3)
    val zipRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)
    zipRDD.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}