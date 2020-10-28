package com.ym123.rdd.rddtwomethod

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**从外部存储系统的数据集创建
 *
 * @author ymstart
 * @create 2020-09-22 18:21
 */
object Test02_CreateRDD02_file {
  def main(args: Array[String]): Unit = {
    //1.配置信息
    val conf:SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    //2.获取信息
    val sc:SparkContext = new SparkContext(conf)
    //3.读取文件
    val lineWordRdd: RDD[String] = sc.textFile("input")
    lineWordRdd.foreach(println)
    sc.stop()
  }
}
