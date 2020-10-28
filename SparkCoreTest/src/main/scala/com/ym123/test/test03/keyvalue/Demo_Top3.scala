package com.ym123.test.test03.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-25 9:10
 */
object Demo_Top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val fileRDD: RDD[String] = sc.textFile("input/agent.txt")

    //3.2取出文件中的省份 及广告 string =>(prv-adv,1)
    val prv_advToOne: RDD[(String, Int)] = fileRDD.map {
      line => {
        val word: Array[String] = line.split(" ")
        (word(1) + "-" + word(4), 1)
      }
    }

    //3.3(prv-adv,1)=>(prv-adv,sum)
    val prv_sumToSum: RDD[(String, Int)] = prv_advToOne.reduceByKey(_+_)

    //3.4(prv-adv,sum)=>(prv,(adv ,sum))
    val prvAndadv_sum: RDD[(String, (String, Int))] = prv_sumToSum.map(
      (a: (String, Int)) => {
        val strings = a._1.split("-")
        (strings(0), (strings(1), a._2))
      }
    )
    //3.5(prv,(adv ,sum))=>(prv,Iterator[(adv,sum)])
    prvAndadv_sum



    //4.关闭连接
    sc.stop()
  }
}
