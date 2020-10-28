package com.ym123.test.test02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**9、需求说明：创建一个RDD，过滤出对2取余等于0的数据
 * @author ymstart
 * @create 2020-09-24 9:03
 */
object Test09 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val filterRDD: RDD[Int] = rdd.filter(_%2 ==0)
    filterRDD.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
