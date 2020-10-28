package com.ym123.rdd.sparkcode

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author xingcang
 * @create 2020-09-28 10:14
 */
object Top10Session {
    def main(args: Array[String]): Unit = {
        // 1. make sparkConf and set appName, master
        val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")

        // 2. create spark context
        val sc = new SparkContext(conf)

        // 3. read data
        val data1Rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

        // 4. map userVisitAction
        val data2Rdd: RDD[UserVisitAction] = data1Rdd.map {
            line => {
                val lineString: Array[String] = line.split("_")
                UserVisitAction(
                    lineString(0),
                    lineString(1).toLong,
                    lineString(2),
                    lineString(3).toLong,
                    lineString(4),
                    lineString(5),
                    lineString(6).toLong,
                    lineString(7).toLong,
                    lineString(8),
                    lineString(9),
                    lineString(10),
                    lineString(11),
                    lineString(12).toLong
                )
            }
        }

        // 5. flatMap() 取出所有的商品ID
        val actionsRdd: RDD[(String, CategoryCountInfo)] = data2Rdd.flatMap {
            action => {
                val actions: ListBuffer[(String, CategoryCountInfo)] = mutable.ListBuffer[(String, CategoryCountInfo)]()
                if (action.click_category_id != -1) {
                    actions.append((action.click_category_id.toString,
                        CategoryCountInfo(action.click_category_id.toString, 1L, 0L, 0L)))
                    actions
                } else if (action.order_category_ids != "null") {
                    val ids: Array[String] = action.order_category_ids.split(",")
                    for (id <- ids) {
                        actions.append((id, CategoryCountInfo(id, 0L, 1L, 0L)))
                    }
                    actions
                } else if (action.pay_category_ids != "null") {
                    val ids: Array[String] = action.pay_category_ids.split(",")
                    for (id <- ids) {
                        actions.append((id, CategoryCountInfo(id, 0L, 0L, 1L)))
                    }
                    actions
                } else
                    Nil
            }
        }

        // 6. reduceByKey
        val allActRdd: RDD[(String, CategoryCountInfo)] = actionsRdd.reduceByKey {
            (left, right) => {
                left.clickCount += right.clickCount
                left.orderCount += right.orderCount
                left.payCount += right.payCount
                left
            }
        }

        //7.  take top10
        val top10Category: Array[String] = allActRdd.sortBy(elem => (elem._2.clickCount, elem._2.orderCount, elem._2.payCount), false)
            .take(10).map(_._2.categoryId)


        val ids: Broadcast[Array[String]] = sc.broadcast(top10Category)

        // 8. top session in each id
        data2Rdd.filter(action => ids.value.contains(action.click_category_id.toString))
            .map{
                click => ((click.click_category_id.toString, click.session_id), 1L)
                }
            .reduceByKey(_ + _)
            .map{
                session =>
                    (session._1._1, (session._1._2, session._2))
            }.groupByKey().mapValues{
            data => data.toList.sortBy(_._2).reverse.take(10)
        }.foreach(println)

        // 9. top city in each id
        data2Rdd.filter(action => ids.value.contains(action.click_product_id.toString)).map(
            city => ((city.click_product_id.toString, city.city_id.toString), 1L)
        ).reduceByKey(_ + _)
                .map{
                    city => (city._1._1, (city._1._2, city._2))
                }.groupByKey().mapValues{
            city => city.toList.sortBy(_._2).reverse.take(10)
        }.foreach(println)


        // 10. conversion rate

        val pages: Broadcast[ListBuffer[Int]] = sc.broadcast(ListBuffer(1,2,3,4,5,6,7))

        //分母
        val denominator: RDD[(Long, Long)] = data2Rdd.filter(page => pages.value.contains(page.page_id)).map(data => (data.page_id, 1L))
            .reduceByKey(_ + _)

        val data4Rdd: RDD[((Long, Long), Long)] = data2Rdd.groupBy(_.session_id).flatMap {
            data =>
                val actions: List[UserVisitAction] = data._2.toList.sortBy(_.action_time)
                val pages1: List[Long] = actions.map(_.page_id)
                val pagesToPages: List[(Long, Long)] = pages1.zip(pages1.tail)
                val pagesToPages2: List[(Long, Long)] = pagesToPages.filter(data => data._2 - data._1 == 1)
                val data3: mutable.Map[(Long, Long), Long] = mutable.Map[(Long, Long), Long]()
                for (elem <- pagesToPages2) data3(elem) = data3.getOrElse(elem, 0L) + 1
                data3
        }

        val numerator: RDD[(Long, Long)] = data4Rdd
            .reduceByKey(_ + _)
            .map(data => (data._1._1, data._2))

        val list1: List[(Long, Long)] = denominator.collect().toList
        val list2: List[(Long, Long)] = numerator.collect().toList

        for (elem1 <- list1){
            for (elem2 <- list2) {
                if(elem1._1 == elem2._1) {
                    println("page: " + elem1._1 + ", conversion rate: "  + (elem2._2 + 0.0) / elem1._2)
                }
            }
        }

        // end: stop sc
        sc.stop()
    }
}