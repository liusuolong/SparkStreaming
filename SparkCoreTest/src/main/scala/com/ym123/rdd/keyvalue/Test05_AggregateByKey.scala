package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**按K处理分区内和分区间逻辑
 * @author ymstart
 * @create 2020-09-24 11:50
 */
object Test05_AggregateByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("hi",2),("hello",2),("jerry",1),("二狗子",1),("jerry",1)),2)
    //先取出每个分区的k对应的最大v 在将每个分区相同k的v相加
    rdd.aggregateByKey(0)((a:Int,b:Int)=>{
      math.max(a,b)
    },_+_).collect().foreach(println)

    val ints = Array(1,2,3,4,5)
    ints.sortWith(  (a,b)=>{
      a<b
    }).foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
