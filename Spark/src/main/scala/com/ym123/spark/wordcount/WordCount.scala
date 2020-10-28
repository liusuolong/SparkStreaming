package com.ym123.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-22 9:58
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //配置信息
    val conf:SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    //提交入口
    val sc:SparkContext = new SparkContext(conf)
    //读取信息
    /*val lineRdd:RDD[String] = sc.textFile("E:\\VWwareIDEA\\Spark\\input\\1.txt")
    //扁平映射
    val wordRdd:RDD[String] = lineRdd.flatMap(_.split(" "))
    val wordToRdd:RDD[(String,Int)] = wordRdd.map((_,1))
    val wordSum:RDD[(String,Int)] = wordToRdd.reduceByKey(_+_)
    //打印到控制台
    val wordCount = wordSum.collect()
    wordCount.foreach(println)*/

    //sc.textFile("E:\\VWwareIDEA\\Spark\\input\\1.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
    sc.stop()
  }
}
