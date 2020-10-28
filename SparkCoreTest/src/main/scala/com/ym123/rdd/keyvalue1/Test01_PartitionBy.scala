package com.ym123.rdd.keyvalue1

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**按K重新分区
 * @author ymstart
 * @create 2020-09-24 14:36
 */
object Test01_PartitionBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("hi",1),("hello",1),("he",1)),4)
    val HashPartitionRDD: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(2))
    HashPartitionRDD.collect().foreach(println)

    HashPartitionRDD.mapPartitionsWithIndex(
      (index,datas)=>datas.map((index,_))
    ).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
