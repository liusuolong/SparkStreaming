package com.ym123.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-24 15:57
 */
object Test07_CombineByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",3),("b",5),("a",10),("b",4)),2)
    //相同k的值进行累加,并记录k出现的次数
    val combinerByKeyRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(

      /**
       * def combineByKey[C](
       * createCombiner: V => C,
       * mergeValue: (C, V) => C,
       * mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
       * combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
       * }
       */
      //取元组的v 并计数
      (_, 1),
      //元组第一位与v相加 ，元组第二位自增1
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      //元组第一位与第二位进行累加
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    //combinerByKeyRDD.collect().foreach(println)
    /*
    b总和为9 出现2次
    (b,(9,2))
    (a,(13,2))
     */
    //计算均值
    combinerByKeyRDD.map{
      case (key,value)=>{
        (key,value._1 /value._2.toDouble)
      }
    }.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
