package com.ym123.sparksql.test01

import org.apache.parquet.schema.Types.ListBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author ymstart
 * @create 2020-09-30 9:56
 */
object Test11_SQL {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql(
      """
        |select ci.area,
        |       ci.city_name,
        |       p.product_name,
        |       v.click_product_id
        |from user_visit_action v
        |         join city_info ci
        |              on v.city_id = ci.city_id
        |         join product_info p
        |              on v.click_product_id = p.product_id
        |where click_product_id > -1
        |""".stripMargin).show(false)
    //show(false) 去掉...
    // 5 释放资源
    spark.stop()
  }
}

/*
total 点击总数
cityMap Map[(cityName,点击数)]
 */
case class Buffer(var total:Long,var cityMap:mutable.Map[String,Long])

class DefineUDAF extends Aggregator[String,Buffer,String]{

  override def zero: Buffer = Buffer(0L,mutable.Map[String,Long]())

  override def reduce(b: Buffer, city: String): Buffer = {
    /*
    输入 -> buffer
     */

    //总点击数
    b.total += 1
    //每个城市点击次数
    val cityCount: Long = b.cityMap.getOrElse(city,0L) +1
    //写回
    b.cityMap.update(city,cityCount)
    b
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    //统计城市点击总数
    b1.total += b2.total
    //[(cityName,点击数)] 统计相同城市的点击总数数
    b2.cityMap.foreach{
      case(city,count)=>{
        val mapCount: Long = b1.cityMap.getOrElse(city,0L) + count
        b1.cityMap.update(city,mapCount)
      }
    }
    b1
  }

  override def finish(buffer: Buffer): String = {
    //将数据添加到集合中
    val remarkList: ListBuffer[String] = ListBuffer[String]()
    val cityMap: mutable.Map[String, Long] = buffer.cityMap
    //累加
    var sum = 0

    //排序
    val sortList: List[(String, Long)] = buffer.cityMap.toList.sortBy(_._2).take(2)

    sortList.foreach{
      case(city,count)=>{
        val r = count * 100 / buffer.total
        remarkList.append(city +" " + r + "%")
        sum += r
      }
    }
    //城市数 > 2
    if(cityMap.size>2){
      remarkList.append("其他" +(100 -sum) +"%")
    }
    remarkList.mkString(",")
  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}
