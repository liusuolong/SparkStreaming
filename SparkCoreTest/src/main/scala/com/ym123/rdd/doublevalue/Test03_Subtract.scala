package com.ym123.rdd.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**差集
 *
 * @author ymstart
 * @create 2020-09-23 18:18
 */
object Test03_Subtract {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(1 to 5)
    val rdd2: RDD[Int] = sc.makeRDD(4 to 8)
    /**
     * rdd1 -rdd2 剩下的东西
     */

    val subtractRDD: RDD[Int] = rdd1.subtract(rdd2)
    subtractRDD.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
