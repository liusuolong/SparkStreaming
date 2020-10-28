package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-23 14:25
 */
object Test06_GroupBy_WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello java","hello scala","hello spark","hbase","zookeeper"))
    //                              word=>(word,1)
    val wordCount: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map({
      case (word, list) => {
        (word, list.size)
      }
    })
    wordCount.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
