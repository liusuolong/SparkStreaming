package com.ym123.sparksql.test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author ymstart
 * @create 2020-09-29 19:34
 */
object Test09_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    //连接外部Hive
    spark.sql("show tables").show()
    //需要开启enableHiveSupport() 和指定 用户 atguigu
    //spark.sql("create table user1(id int,name string)")
    spark.sql("insert into user1 values(1,'zs')")
    spark.sql("select * from user1").show()

    // 5 释放资源
    spark.stop()
  }
}
