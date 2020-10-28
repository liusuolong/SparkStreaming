package com.ym123.rdd.dependcency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**查看血缘关系 toDeBugString
 * @author ymstart
 * @create 2020-09-25 14:43
 */
object Test01_toDeBugString {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/1.txt")
    println(rdd.toDebugString)

    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
    println(wordRDD.toDebugString)

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.toDebugString)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(reduceRDD.toDebugString)

    reduceRDD.collect()

    //4.关闭连接
    sc.stop()
  }
}
