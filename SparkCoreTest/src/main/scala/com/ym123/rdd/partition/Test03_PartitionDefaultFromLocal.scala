package com.ym123.rdd.partition

import org.apache.spark.{SparkConf, SparkContext}

/**默认分区 从本地文件读取
 * @author ymstart
 * @create 2020-09-22 19:38
 */
object Test03_PartitionDefaultFromLocal {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.textFile("input")
    //分区默认值是cpu核数与2取最小值
    rdd.saveAsTextFile("output3")
    //4.关闭连接
    sc.stop()
  }
}
