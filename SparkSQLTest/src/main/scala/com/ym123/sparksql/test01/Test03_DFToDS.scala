package com.ym123.sparksql.test01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-29 15:04
 */
object Test03_DFToDS {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val fileRDD: RDD[String] = sc.textFile("input/user.txt")
    //读取数据
    val df: DataFrame = spark.read.json("input/user.json")

    val userRDD: RDD[(String, Long)] = fileRDD.map {
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), datas(1).trim.toLong)
      }
    }
    import spark.implicits._

    //DF => DS
    val ds: Dataset[User] = df.as[User]
    ds.show()
    println("===================")
    //DS  =>  DF
    ds.toDF().show()

    //4.关闭连接
    sc.stop()
  }
}
