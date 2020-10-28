package com.ym123.rdd.value1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**存在shuffle过程
 * @author ymstart
 * @create 2020-09-24 21:17
 */
object Test06_GroupBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,4,5,8,6,7,10))
    val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_%2)
    groupByRDD.collect().foreach(println)
    println("=========================")
    val rdd1: RDD[String] = sc.makeRDD(List("hive","hello","tom","jerry"))
    //按第单词第一个字母分组
    val groupRDD1: RDD[(String, Iterable[String])] = rdd1.groupBy(_.substring(0,1))
    groupRDD1.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
