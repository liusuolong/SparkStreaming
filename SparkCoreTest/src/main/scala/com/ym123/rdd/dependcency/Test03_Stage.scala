package com.ym123.rdd.dependcency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 一个sc 一个application
 * 一个行动算子就是一个job
 * 一个宽依赖(shuffle)=2个stage
 * 最后一个RDD的分区个数 = Task
 *
 * @author ymstart
 * @create 2020-09-27 0:11
 */
object Test03_Stage {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建RDD
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2),2)

    val resultRDD: RDD[(Int, Int)] = dataRDD.map((_,1)).reduceByKey(_+_)

    resultRDD.collect().foreach(println)

    resultRDD.saveAsTextFile("output")

    Thread.sleep(1000000000)

    //4.关闭连接
    sc.stop()
  }
}
