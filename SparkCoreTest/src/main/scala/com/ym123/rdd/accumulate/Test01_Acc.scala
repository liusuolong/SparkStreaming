package com.ym123.rdd.accumulate

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-27 22:34
 */
object Test01_Acc {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 数据
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),1)

    //定义累加器
    val sum = sc.longAccumulator("sum1")

    //使用累加器 添加数据
    rdd.foreach{
      case (a,count)=>{
        if(a =="a")
        sum.add(count)
        else
          0
      }
    }
    //调用累加器的值
    println(sum.value)

    //4.关闭连接
    sc.stop()
  }
}
