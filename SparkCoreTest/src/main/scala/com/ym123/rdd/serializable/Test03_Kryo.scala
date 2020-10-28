package com.ym123.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 即使使用Kryo序列化，也要继承Serializable接口。
 * 或者使用样例类
 *
 * @author ymstart
 * @create 2020-09-26 23:20
 */
object Test03_Kryo {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark serializer","org.apache.spark.serializer.KryoSerializer")
      //注册需要使用kryo序列化的自定义类
      .registerKryoClasses(Array(classOf[Searcher]))

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello scala","hello java","scala","hello spark"))

    val searcher = new Searcher("hello")
    searcher.getMatchedRDD1(rdd).foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

//自定义样例类
case class Searcher(val query:String){
  def isMatch(s: String) = {
    s.contains(query)
  }

  def getMatchedRDD1(rdd: RDD[String]) = {
    rdd.filter(isMatch)
  }

  def getMatchedRDD2(rdd: RDD[String]) = {
    val q = query
    rdd.filter(_.contains(q))
  }
}