package com.ym123.sparksql.test01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**将数据写入sql中
 * @author ymstart
 * @create 2020-09-29 18:34
 */
object Test08_MySql_Write {
  def main(args: Array[String]): Unit = {
   // 1 创建上下文环境配置对象
   val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

   // 2 创建SparkSession对象
   val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //准备数据
    //id 为主键 不能修改
    val datas: RDD[User1] = spark.sparkContext.makeRDD(List(User1(10010,"zs"),User1(10011,"ls")))

    import spark.implicits._

    val ds: Dataset[User1] = datas.toDS

    ds.write.format("jdbc")
      .option("url","jdbc:mysql://hadoop002:3306/gmall")
        .option("user","root")
        .option("password","123456")
        .option("dbtable","user_info")
        .mode(SaveMode.Append)
        .save()
    // 5 释放资源
   spark.stop()
  }
}
case class User1(id:Long,name:String){

}