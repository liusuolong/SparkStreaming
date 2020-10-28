package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**group 存在shuffle过程
 * @author ymstart
 * @create 2020-09-23 13:43
 */
object Test06_GroupBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)

    val groupRdd: Array[(Int, Iterable[Int])] = rdd.groupBy(_ % 2).collect()
    groupRdd.foreach(println)

    val rdd1: RDD[String] = sc.makeRDD(List("hello","hi","merry","tom","jerry","jack"))
    rdd1.groupBy(_.substring(0,1)).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}