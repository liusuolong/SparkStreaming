package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**每一个分区 => 数组 =>放入新的RDD中
 *
 * @author ymstart
 * @create 2020-09-23 11:28
 */
object Test05_Glom {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)
    val glomRdd: RDD[Array[Int]] = rdd.glom()
    //println(glomRdd.collect().mkString(" "))

    //求出每一个分区的最大值
    rdd.glom().map(_.max).collect().foreach(println)

    //分区最大值之和
    //法一
    //println(rdd.mapPartitions(iter => List(iter.max).iterator).sum())
    //法二
    //println(rdd.glom().flatMap(array => List(array.max).iterator).sum())

    //4.关闭连接
    sc.stop()
  }
}
