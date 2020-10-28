package com.ym123.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**会增加血缘关系,不会改变原有的血缘关系
 * @author ymstart
 * @create 2020-09-27 1:00
 */
object Test01_cache {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/1.txt")

    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map {
      word => {
        println("******************")
        (word, 1)
      }
    }
    wordToOneRDD.collect().foreach(println)
    println("============================")
    //数据缓存
    wordToOneRDD.cache()
    //触发逻辑
    wordToOneRDD.collect().foreach(println)




    //4.关闭连接
    sc.stop()
  }
}
