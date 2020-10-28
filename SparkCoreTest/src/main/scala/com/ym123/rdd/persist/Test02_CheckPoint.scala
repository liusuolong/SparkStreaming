package com.ym123.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**会移除所有的父RDD关系
 * 为了数据安全会从血缘关系最开始执行一次
 * @author ymstart
 * @create 2020-09-27 1:08
 */
object Test02_CheckPoint {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //设置检查点存放路径
    sc.setCheckpointDir("./checkpoint")

    val rdd: RDD[String] = sc.textFile("input/1.txt")
    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val wordToOneRDD: RDD[(String, Long)] = wordRDD.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }

    //缓存
    wordToOneRDD.cache()
    //检查点
    wordToOneRDD.checkpoint()
    //触发
    wordToOneRDD.collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
