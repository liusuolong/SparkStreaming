package com.wordcounttest_01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author ymstart
 * @create 2020-10-30 20:22
 */
object WordCountTest_02 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建sparkStreaming上下文环境对象
    val ssc = new StreamingContext(conf,Seconds(3))

    //创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //创建QueueInputStream
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue,oneAtATime = false)

    //处理队列中的数据
    inputStream.map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    for(i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 5 ,10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

    //4.关闭连接
    ssc.stop()
  }
}
