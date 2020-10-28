package com.ym123.sparksql.test01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ymstart
 * @create 2020-09-29 12:55
 */
object Test01_RDDToDF {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    //2.1创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    // 2.2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //3.1获取数据
    val fileRDD: RDD[String] = sc.textFile("input/user.txt")

    //数据准备
    val UserRDD: RDD[(String, Long)] = fileRDD.map {
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), datas(1).trim.toLong)
      }
    }

    //转换必须导包
    import spark.implicits._

    //RDD => DateFrame  (手动)
    UserRDD.toDF("name","age").show
    println("============================")
    UserRDD.toDF().show()

    println("============================")
    //RDD => DateFrame  (样例类)
    val frame: DataFrame = UserRDD.map {
      case (name, age) => User(name, age)
    }.toDF()

    //DF => RDD
    frame.rdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()
    // 5 释放资源
    spark.stop()
  }
}
case class User(name:String,age:Long){

}
