package com.ym123.rdd.value1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 22:52
 */
object Test06_WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/1.txt")
    rdd
      /**
       *按空格将单词切开
       * hello
       * hi
       * my
       * name
       * is
       * chuzihang
       * he
       * name
       * is
       * lumingfei
       */
      .flatMap(_.split(" "))

      /**
       * (is,CompactBuffer(is, is))
       * (hello,CompactBuffer(hello))
       * (my,CompactBuffer(my))
       * (lumingfei,CompactBuffer(lumingfei))
       * (name,CompactBuffer(name, name))
       * (hi,CompactBuffer(hi))
       * (he,CompactBuffer(he))
       * (chuzihang,CompactBuffer(chuzihang))
       */
      .groupBy(word=>word)

      /**
       * (is,2)
       * (hello,1)
       * (my,1)
       * (lumingfei,1)
       * (name,2)
       * (hi,1)
       * (he,1)
       * (chuzihang,1)
       */
      .map(tuple=>(tuple._1,tuple._2.size))
      .collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
