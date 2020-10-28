package com.ym123.test.test05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-27 8:57
 */
object Top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/agent.txt")

    //3.2 string=>(prv_advToOne)
    val prv_advToOne: RDD[(String, Int)] = rdd.map {
      line => {
        val lineRDD: Array[String] = line.split(" ")
        (lineRDD(1) + "-" + lineRDD(4), 1)
      }
    }

    //(prv-advToOne) =>(prv-adv,sum)
    val prv_advToSum: RDD[(String, Int)] = prv_advToOne.reduceByKey(_+_)

    //3.4(prv-adv,sum)=>(prv,(adv,sum))
    val prvToAdv_Sum: RDD[(String, (String, Int))] = prv_advToSum.map {
      case (prv_adv, sum) => {
        val prvToAdv: Array[String] = prv_adv.split(" ")
        (prvToAdv(0), (prvToAdv(1), sum))
      }
    }

    //3.5(prv,(adv ,sum))=>(prv,Iterator[(adv,sum)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvToAdv_Sum.groupByKey()

    //取top3
    groupRDD.mapValues(
      data => data.toList.sortWith(
        (curr,prv) =>curr._2 > prv._2
      ).take(3)
    ).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
