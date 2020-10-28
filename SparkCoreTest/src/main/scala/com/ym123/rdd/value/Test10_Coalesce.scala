package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**合并分区
 *两种方式：执行shuffle和不执行shuffle
 * @author ymstart
 * @create 2020-09-23 15:19
 */
object Test10_Coalesce {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /**
     * 非shuffle方式
     * for (i <- 0 until maxPartitions) {
     * val rangeStart = ((i.toLong * prev.partitions.length) / maxPartitions).toInt
     * val rangeEnd = (((i.toLong + 1) * prev.partitions.length) / maxPartitions).toInt
     *
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)
    val coalesceRDD: RDD[Int] = rdd.coalesce(2,false)
    //coalesceRDD.collect().foreach(println)

    //打印分区数据
    /*val indexRDD: RDD[Int] = coalesceRDD.mapPartitionsWithIndex(
      (index, datas) => {
        datas.foreach(data => {
          println(index + "=>" + data)
        })
        datas
      })
    indexRDD.collect()*/
    coalesceRDD.mapPartitionsWithIndex(
      (index,datas)=>{
        datas.map((index,_))
      }
    ).collect().foreach(println)

    /**
     * shuffle方式
     */
    val shuffleRdd: RDD[Int] = sc.makeRDD(Array(2,4,6,8,10,10,22,11),4)
    //shuffleRdd.coalesce(2,true).collect().foreach(println)
    val shuffleRDD1: RDD[Int] = shuffleRdd.coalesce(2,true)

   /* val indexRDD: RDD[Int] = shuffleRDD1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.foreach(data => {
          println(index + "=>" + data)
        })
        datas
      })*/

    /**
     * 1=>4
     * 0=>2
     * 0=>8
     * 1=>6
     * 1=>10
     * 0=>10
     * 1=>11
     * 0=>22
     */
    //indexRDD.collect()


    //4.关闭连接
    sc.stop()
  }
}
