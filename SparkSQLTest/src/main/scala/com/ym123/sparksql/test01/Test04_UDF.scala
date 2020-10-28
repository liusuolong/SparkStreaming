package com.ym123.sparksql.test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ymstart
 * @create 2020-09-29 15:14
 */
object Test04_UDF {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //读取数据
    val df: DataFrame = spark.read.json("input/user.json")
    //创建df临时视图
    df.createOrReplaceTempView("user")
    //error
    //df.createOrReplaceGlobalTempView("user")
    //注册UDF
    spark.udf.register("myName",(name:String)=>"Name:"+ name)
    //自定义UDF函数
    spark.sql("select myName(name),age from user").show()

    // 5 释放资源
    spark.stop()
  }
}
