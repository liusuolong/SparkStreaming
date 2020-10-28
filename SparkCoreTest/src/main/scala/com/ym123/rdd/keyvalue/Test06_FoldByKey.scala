package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**分区内和分区间逻辑相同
 * @author ymstart
 * @create 2020-09-24 13:58
 */
object Test06_FoldByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("hi",2),("hello",2),("scala",1),("java",1)))
    //直接将不同分区的相同k的v进行累加

    rdd.foldByKey(0)(_+_).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
