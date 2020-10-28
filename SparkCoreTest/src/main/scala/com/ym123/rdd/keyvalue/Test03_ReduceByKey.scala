package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**按K进行聚合
 * 先进行combine 再shuffle
 * @author ymstart
 * @create 2020-09-23 19:16
 */
object Test03_ReduceByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("hello",2),("hi",3),("tom",1),("hello",1)))
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)
    reduceByKeyRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
