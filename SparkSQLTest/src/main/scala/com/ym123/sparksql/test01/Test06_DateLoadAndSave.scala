package com.ym123.sparksql.test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ymstart
 * @create 2020-09-29 16:32
 */
object Test06_DateLoadAndSave {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    /*
    加载数据
     */
    //直接读出数据
    val df: DataFrame = spark.read.json("input/user.json")
    df.show()
    //指定加载数据类型
    spark.read.format("json").load("input/user.json").show()

    /*
    保存数据
    默认为parquet文件
     */
    //df.write.save("output")
    //指定保存格式
    //df.write.format("json").save("output1")
    //直接指定格式,直接保存
    //df.write.json("output2")

    //向文件追加内容 会生成新的文件
    //df.write.mode("append").json("output2")

    //文件存在忽略
    df.write.mode("ignore").json("output")

    //文件存在覆盖
    df.write.mode("overwrite").json("output")

    //文件已存在抛异常  already exists
    //df.write.mode("error").json("output")

    // 5 释放资源
    spark.stop()
  }
}