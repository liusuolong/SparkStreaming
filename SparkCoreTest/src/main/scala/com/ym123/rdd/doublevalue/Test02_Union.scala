package com.ym123.rdd.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**并集
 * @author ymstart
 * @create 2020-09-23 18:11
 */
object Test02_Union {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd2: RDD[Int] = sc.makeRDD(List(2,3,7,8,5))
    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    unionRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
