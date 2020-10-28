package com.ym123.rdd.keyvalue1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-26 21:38
 */
object Test12_Top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 读入数据
    val rdd: RDD[String] = sc.textFile("input/agent.log")
    /**
     * 时间戳，省份，城市，用户，广告
     * 1516609143869 5 2 95 24
     * 需求：统计出每一个省份广告被点击次数的top3
     */
    //3.2取出文件中的省份 及广告 string =>(prv-adv,1)
    val prv_advToOne: RDD[(String, Int)] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    )
    //3.3(prv-adv,1) =>(prv-adv,sum)
    val prv_advToSum: RDD[(String, Int)] = prv_advToOne.reduceByKey((a: Int, b: Int) => (a + b))
    //prv_advToSum.collect().foreach(println) // (1-16,12)

    //3.4(prv-adv,sum)=>(prv,(adv,sum))
    val prv_advAndSum: RDD[(String, (String, Int))] = prv_advToSum.map {
      case (prv_adv, sum) => {
        val data: Array[String] = prv_adv.split("-")
        (data(0), (data(1), sum))
      }
    }
    //3.5(prv,(adv ,sum))=>(prv,Iterator[(adv,sum)])
    val groupByPrv: RDD[(String, Iterable[(String, Int)])] = prv_advAndSum.groupByKey()
    //3.6 取不同省份的top3
    groupByPrv.mapValues {
          //对迭代器排序
      data => data.toList.sortWith(
        //left 为当前迭代器元素 rigth为下一次数据
        (left,right)=>{left._2 > right._2}
      ).take(3)
    }.collect().foreach(println)








    //4.关闭连接
    sc.stop()
  }
}
