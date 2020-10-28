package com.ym123.rdd2.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-10-26 20:16
 */
object Test03_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),2)

    //获取每个分区最大的数
    rdd.mapPartitionsWithIndex(
      (index,iter)=>{
        List(index,iter.max).iterator
      }
    ).collect().foreach(println)

    //获取1号分区所有的数据
    /*rdd.mapPartitionsWithIndex(
      (index,iter)=>{
        if (index == 1)
          iter
        else
          Nil.iterator
      }
    ).collect().foreach(println)*/

    //4.关闭连接
    sc.stop()
  }
}
