package com.ym123.test.test03.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-25 8:32
 */
object Test03h_MapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("eo",2),("lo",4),("heo",3)),2)
    rdd.mapPartitionsWithIndex(
      (index,data)=>{data.map((index,_))}
    ).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
