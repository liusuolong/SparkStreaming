package com.ym123.sparksql.test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

/**从sql中读取数据
 * @author ymstart
 * @create 2020-09-29 18:20
 */
object Test07_MySql_Read {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //import spark.implicits._
    //3.1通过load方式读取
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop002:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()

    df.createOrReplaceTempView("user")

    //查询数据
    spark.sql("select id,name from user").show()

    // 5 释放资源
    spark.stop()
  }
}
