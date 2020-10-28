package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**自定义分区
 * @author ymstart
 * @create 2020-09-23 18:58
 */
object Test02_SelfDefinePartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1,"hello"),(2,"hi"),(3,"jack")),3)

    rdd.partitionBy(new MyPartition(2))

    //打印分区数据
    val indexRDD: RDD[(Int, (Int, String))] = rdd.mapPartitionsWithIndex(
      (index, datas) => datas.map((index, _))
    )
    indexRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
//自定义分区 可以处理数据倾斜
class MyPartition(i: Int) extends Partitioner{
  //设置分区数
  override def numPartitions: Int = i

  //具体逻辑
  override def getPartition(key: Any): Int = {
    //判断类型
    if(key.isInstanceOf[Int]){
      val ketInt: Int = key.asInstanceOf[Int]
      //分区逻辑
      if(ketInt % 2 == 0) 0 else 1
    }else 0
  }
}
