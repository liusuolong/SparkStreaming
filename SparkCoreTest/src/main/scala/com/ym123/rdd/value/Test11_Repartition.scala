package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**重新分区
 * 会走shuffle ，如果是减少分区，建议使用coalesce的不走shuffle方法
 * 可以增加分区
 * @author ymstart
 * @create 2020-09-23 16:26
 */
object Test11_Repartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,11,22,33,44),4)
    val repartitionRdd: RDD[Int] = rdd.repartition(4)
    val indexRdd: RDD[Int] = repartitionRdd.mapPartitionsWithIndex(
      (index, datas) => {
        //打印每个分区的数据，并带上分区号
        datas.foreach(data => {
          println(index + "=>" + data)
        })
        datas
      }
    )
    indexRdd.collect()
    //4.关闭连接
    sc.stop()
  }
}
