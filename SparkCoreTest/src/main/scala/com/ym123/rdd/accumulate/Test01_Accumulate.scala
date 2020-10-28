package com.ym123.rdd.accumulate

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**系统累加器
 * @author ymstart
 * @create 2020-09-27 9:41
 */
object Test01_Accumulate {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    //声明一个累加器
    val sum1: LongAccumulator = sc.longAccumulator("sum1")
    var sum = 0

    dataRDD.foreach{
      case (a,count) =>{
        //累加器添加数据
        sum1.add(count)

        println(sum1.value)
      }
    }
    println("==================")
    //获取累加器的值
    println(sum1.value)

    //4.关闭连接
    sc.stop()
  }
}
