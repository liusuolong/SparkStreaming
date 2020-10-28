package com.ym123.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-26 22:56
 */
object Test02_MethodAndAttr {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("hive")
    //需要序列化
    //search.getMatch1(rdd).foreach(println)
    //search.getMatch2(rdd).foreach(println)

    val search1 = new Search1("hive")
    search1.getMatch1(rdd).foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
class Search(query:String) extends Serializable{

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 函数序列化案例
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    //rdd.filter(this.isMatch)
    rdd.filter(isMatch) //filter是一个RDD
  }

  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //rdd.filter(x => x.contains(this.query))
    rdd.filter(x => x.contains(query)) //filter 是RDD
    //val q = query   //基本变量 自带序列化
    //rdd.filter(x => x.contains(q))
  }
}

//样例类 自带序列化
case class Search1(query:String){

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 函数序列化案例
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    //rdd.filter(this.isMatch)
    rdd.filter(isMatch) //filter是一个RDD
  }

  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //rdd.filter(x => x.contains(this.query))
    rdd.filter(x => x.contains(query)) //filter 是RDD
    //val q = query   //基本变量 自带序列化
    //rdd.filter(x => x.contains(q))
  }
}
