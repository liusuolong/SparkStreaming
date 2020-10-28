package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**一进多出
 * @author ymstart
 * @create 2020-09-23 11:20
 */
object Test04_FlatMap {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2,3),List(4,5,6),List(7,8,9)),2)

    val rdd1: Array[Int] = rdd.flatMap(list =>list).collect()
    println(rdd1.mkString(" "))
    //4.关闭连接
    sc.stop()
  }
}
