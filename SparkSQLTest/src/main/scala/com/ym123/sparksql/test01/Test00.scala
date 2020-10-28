package com.ym123.sparksql.test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ymstart
 * @create 2020-09-29 23:22
 */
object Test00 {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取文件
    val df: DataFrame = spark.read.json("input/user.json")

    df.show()

    // 5 释放资源
    spark.stop()
  }
}
