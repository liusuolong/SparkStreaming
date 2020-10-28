package com.ym123.test.test02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**7、需求说明：创建一个2个分区的RDD，并将每个分区的数据放到一个数组，求出每个分区的最大值
 * @author ymstart
 * @create 2020-09-24 8:53
 */
object Test07 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    //将数据放入一个数组
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    //取出每个分区的最大值
    glomRDD.map(_.max).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
