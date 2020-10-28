package com.ym123.rdd.dependcency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**获取依赖关系
 * @author ymstart
 * @create 2020-09-26 23:54
 */
object Test02_dependencies {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/11.txt")
    println(rdd.dependencies)
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    println(flatMapRDD.dependencies)
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_,1))
    println(mapRDD.dependencies)
    val reducerRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(reducerRDD.dependencies)
    reducerRDD.collect()
    //4.关闭连接
    sc.stop()
  }
}
