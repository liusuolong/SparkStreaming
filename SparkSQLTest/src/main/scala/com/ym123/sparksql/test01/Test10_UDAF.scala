package com.ym123.sparksql.test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 * @author ymstart
 * @create 2020-09-30 8:38
 */
object Test10_UDAF {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/user.json")

    df.createOrReplaceTempView("grade")

    spark.udf.register("avg",functions.udaf(new myAvg))

    spark.sql("select avg(age) from grade").show()

    // 5 释放资源
    spark.stop()
  }
}

case class  Buff(var sum:Long,var count:Long){

}

class myAvg extends Aggregator[Long,Buff,Long]{
  override def zero: Buff = Buff(0L,0L)

  override def reduce(b: Buff, a: Long): Buff = {
    b.sum += a
    b.count += 1
    b
  }

  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: Buff): Long = {
    reduction.sum / reduction.count
  }

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
