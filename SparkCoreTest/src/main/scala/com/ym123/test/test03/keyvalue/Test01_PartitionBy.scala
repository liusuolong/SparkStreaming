package com.ym123.test.test03.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-25 8:54
 */
object Test01_PartitionBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("heo",2),("llo",5),("ho",4),("heo",5)),4)
    rdd.partitionBy(new HashPartitioner(2)).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
