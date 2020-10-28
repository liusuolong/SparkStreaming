package com.ym123.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**有闭包需要需要序列化
 * @author ymstart
 * @create 2020-09-25 13:19
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val student1 = new Student
    student1.name = "Tom"

    val student2 = new Student
    student2.name = "Jerry"
    val studentRDD: RDD[Student] = sc.makeRDD(List(student1,student2))
    val stuRDD: RDD[Student] = sc.makeRDD(List())

    //NotSerializableException
    //studentRDD.foreach(student1=>println(student1.name))
    //不会报错
    stuRDD.foreach(student1=>println(student1.name))

    //4.关闭连接
    sc.stop()
  }
}
class Student extends /*Serializable*/ {
  var name:String = _
}
