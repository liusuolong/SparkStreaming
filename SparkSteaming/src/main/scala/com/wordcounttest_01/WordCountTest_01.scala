package com.wordcounttest_01

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-10-30 19:50
 */
object WordCountTest_01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建sparkStreaming上下文环境对象
    // Seconds(3)批次处理时间 3s
    val ssc = new StreamingContext(conf,Seconds(3))

    //操作数据源,从端口获取一行数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop002",9999)

    //对获取的数据扁平化
    val flatMapDS: DStream[String] = socketDS.flatMap(_.split(" "))

    //结构化
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)
    reduceDS.print()

    //启动采集器
    ssc.start()

    //4.关闭连接
    //ssc.stop()

    //等待采集器结束,关闭上下文环境对象
    ssc.awaitTermination()
  }
}
