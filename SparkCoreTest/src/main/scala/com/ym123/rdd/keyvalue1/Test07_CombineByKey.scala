package com.ym123.rdd.keyvalue1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-26 20:50
 */
object Test07_CombineByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello", 2), ("hello",1),("hi", 2), ("yy", 2), ("hh", 2), ("hi", 1), ("hello", 2)), 2)

    rdd.combineByKey(
      //("hello", 2) => (2,1)
      (_, 1),
      //(2,1)  => (2,1) + 1 => (3,2)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      //将不同分区的相同的k进行聚合
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1 +acc2._1,acc1._2 +acc2._2)
      //(hh,(2,1))    k,(总个数,出现次数)
      //(yy,(2,1))
      //(hello,(5,3))
      //(hi,(3,2))
    ).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
