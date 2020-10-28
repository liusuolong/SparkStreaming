package com.ym123.sparksql.test01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-29 14:54
 */
object Test02_RDDToDS {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //读入数据
    val fileRDD: RDD[String] = sc.textFile("input/user.txt")
    //数据准备
    val userRDD: RDD[(String, Long)] = fileRDD.map {
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), datas(1).trim.toLong)
      }
    }

    import spark.implicits._
    //RRD => DS
    val ds: Dataset[(String, Long)] = userRDD.toDS()
    ds.show()

    println("===================================")
    //DS => RDD
    ds.rdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
