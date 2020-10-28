package com.ym123.rdd.sparkcode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-27 13:41
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")
    rdd.map{
      line => {
        val word: Array[String] = line.split("_")
        word(9)+"-"+word(10)+"-"+word(11)
      }

    }

    //4.关闭连接
    sc.stop()
  }
}
