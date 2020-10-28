package com.ym123.rdd.value1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**重新分区 执行shuffle
 * @author ymstart
 * @create 2020-09-24 23:54
 */
object Test10_Repartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,12,33,44,55,77,66),4)
    rdd.repartition(6).mapPartitionsWithIndex(
      (index,data)=>{data.map((index,_))}
    ).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
