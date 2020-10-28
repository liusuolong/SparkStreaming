package com.ym123.sparksql.test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 * @author ymstart
 * @create 2020-09-29 15:33
 */
object Test05_UDAF {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取数据
    val df: DataFrame = spark.read.json("input/user.json")
    //创建临时视图
    df.createOrReplaceTempView("user")
    //注册
    spark.udf.register("avg",functions.udaf(new MyAvgUDAF))

    spark.sql("select avg(age) from user").show()

    // 5 释放资源
    spark.stop()
  }
}
case class Buff(var sum:Long,var count:Long){

}

/**[Long,Buff,Long]
 * Long 输入数据类型
 * Buff
 * Long 输出数据类型
 */
class MyAvgUDAF extends Aggregator[Long,Buff,Double]{
  override def zero: Buff = Buff(0L,0L)

  //将输入年龄与缓冲区数据进行聚合
  override def reduce(buff: Buff, age: Long): Buff = {
    buff.sum += age
    buff.count += 1
    buff
  }

  //将多个缓冲区的数据聚合
  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.count +=b2.count
    b1
  }

  override def finish(reduction: Buff): Double = {
    reduction.sum/reduction.count.toDouble
  }

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}