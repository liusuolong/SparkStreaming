package com.ym123.rdd2.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**将一个分区的数据变为数组
 * @author ymstart
 * @create 2020-10-26 21:03
 */
object Test05_glom {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))

   val rdd_glom: RDD[Array[Int]] = rdd.glom()
    rdd_glom.collect().foreach(println)

    println(rdd_glom.flatMap(a => List(a.max).iterator).sum())

    //4.关闭连接
    sc.stop()
  }
}
