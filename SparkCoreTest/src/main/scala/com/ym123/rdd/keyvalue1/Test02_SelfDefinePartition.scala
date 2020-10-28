package com.ym123.rdd.keyvalue1

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-25 0:30
 */
object Test02_SelfDefinePartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1,"hello"),(2,"hi"),(3,"jack")),3)
    rdd.partitionBy(new myPatition(2)).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
class myPatition(i: Int) extends Partitioner{
  override def numPartitions: Int = i

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      val i1: Int = key.asInstanceOf[Int]
      if(i1 %2 == 0)  0 else 1
    }else 0
  }
}
