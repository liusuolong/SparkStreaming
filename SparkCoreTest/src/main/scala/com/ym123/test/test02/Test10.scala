package com.ym123.test.test02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**10、需求说明：创建一个RDD（1-10），从中选择放回和不放回抽样
 * @author ymstart
 * @create 2020-09-24 9:06
 */
object Test10 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    //不放回
    val sampleRDD1: RDD[Int] = rdd.sample(false,0.8,1)
    //sampleRDD1.collect().foreach(println)
    //放回
    val sampleRDD2: RDD[Int] = rdd.sample(true,0.5,1)
    sampleRDD2.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
