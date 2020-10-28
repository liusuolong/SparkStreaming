package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**以分区为单位执行map
 * 将每一个分区的数据放入一个迭代器，进行批处理
 * @author ymstart
 * @create 2020-09-23 10:01
 */
object Test02_MapPartitions {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)
    //x=>x.map(_*2)   _.map(_*2) 第一个_表示一个分区的数据
    val rdd1: RDD[Int] = rdd.mapPartitions(_.map(_*2)) //迭代器
    rdd1.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
