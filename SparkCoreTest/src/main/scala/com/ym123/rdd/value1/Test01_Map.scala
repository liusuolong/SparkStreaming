package com.ym123.rdd.value1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**一进一出
 * @author ymstart
 * @create 2020-09-24 14:21
 */
object Test01_Map {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)
    //每个数乘2输出
    rdd.map(_*2).collect().foreach(println)
    println("=================================")

    //读取本地文件
    val rdd1: RDD[String] = sc.textFile("input/agent.log")
    rdd1.map(
      str =>{
        val line: Array[String] = str.split(" ")
        line(1)+"-"+line(4)
      }
    ).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
