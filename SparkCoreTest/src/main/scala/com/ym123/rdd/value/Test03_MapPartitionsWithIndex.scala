package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**带分区号 元组形式
 * @author ymstart
 * @create 2020-09-23 10:21
 */
object Test03_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
   //1.创建SparkConf并设置App名称
   val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

   //2.创建SparkContext，该对象是提交Spark App的入口
   val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)
    /**
     * f: (Int, Iterator[T]) => Iterator[U]
     *
     * Int 为分区号
     * Iterator为迭代器
     */
    val indexRdd: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
     (index, items)=>{items.map((index,_))
     })
    indexRdd.collect().foreach(println)

   println("=========================================")
    //获取第二个分区的数据
    val rdd1: RDD[Int] = rdd.mapPartitionsWithIndex((index1, items1) => {
      if (index1 == 1) {
        items1
      } else {
        Nil.iterator
      }
    })
    println(rdd1.collect().mkString(" "))

   //4.关闭连接
   sc.stop()
  }
}
