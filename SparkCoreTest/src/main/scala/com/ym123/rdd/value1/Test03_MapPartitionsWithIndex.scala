package com.ym123.rdd.value1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 20:30
 */
object Test03_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,8,7),2)
    rdd.mapPartitionsWithIndex(
      (num,ites)=>{
        //获取分区最大值及分区号
        List((num,ites.max)).iterator
      }
    ).collect().foreach(println)
    println("================================")
    //获取第二个分区的数据
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,8,7),2)
    rdd1.mapPartitionsWithIndex(
      //num 为分区数 iters为数据
      (num,iters)=>{
        if(num == 1){
          iters
        }else{
          Nil.iterator
        }
      }
    ).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
