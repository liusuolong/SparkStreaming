package com.ym123.rdd.keyvalue1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-26 20:38
 */
object Test06_FoldByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",2),("hi",2),("yy",2),("hh",2),("hi",1),("hello",2)),2)

    //分区内和分区间的逻辑相同
    val foldRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
    foldRDD.collect().foreach(println)
    println("===========================")

    //查看分区号
    foldRDD.mapPartitionsWithIndex(
      (index,datas)=>datas.map((index,_))
    ).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
