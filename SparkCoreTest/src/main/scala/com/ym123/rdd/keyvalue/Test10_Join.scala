package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 18:28
 */
object Test10_Join {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",2),("hi",1),("hello",3),("oh",2),("hi",1)))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("hi",2),("hello",1),("tom",3),("oh",2),("hi",1)))
    //将两个rdd中相同的key放到一个元组中
    /**
     * join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
     * 但只有一个RDD中有key  则这个会被抛弃
     */
    val joinRDD: RDD[(String, (Int, Int))] = rdd.join(rdd1)
    joinRDD.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
