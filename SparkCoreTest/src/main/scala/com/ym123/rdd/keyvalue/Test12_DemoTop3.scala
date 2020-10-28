package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 18:46
 */
object Test12_DemoTop3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /**
     * 时间戳，省份，城市，用户，广告
     * 1516609143869 5 2 95 24
     * 需求：统计出每一个省份广告被点击次数的top3
     */
      //3.1读入文件
    val file: RDD[String] = sc.textFile("input/agent.log")

    //3.2取出文件中的省份 及广告 string =>(prv-adv,1)
    val prv_advToOne: RDD[(String, Int)] = file.map {
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    }

    //3.3(prv-adv,1)=>(prv-adv,sum)
    val prv_advToSum: RDD[(String, Int)] = prv_advToOne.reduceByKey(_+_)

    //3.4(prv-adv,sum)=>(prv,(adv ,sum))
    val prvAndAdv_Sum: RDD[(String, (String, Int))] = prv_advToSum.map {
      case (pre_adv, sum) => {
        val splitWord: Array[String] = pre_adv.split("-")
        (splitWord(0), (splitWord(1), sum))
      }
    }

    //3.5(prv,(adv ,sum))=>(prv,Iterator[(adv,sum)])
    val groupByPrv: RDD[(String, Iterable[(String, Int)])] = prvAndAdv_Sum.groupByKey()
    //3.6相同省份的广告取top3
    val top3: RDD[(String, List[(String, Int)])] = groupByPrv.mapValues {
      datas => datas.toList.sortWith(
        (left,right)=>{
          left._2 > right._2
        }
      ).take(3)
    }
    top3.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
