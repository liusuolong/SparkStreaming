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
        // 将读到的行数据切片，以样例类的形式保存
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
        // flatMap()使用时，元素需要是可迭代对象，因此将其封装在一个ListBuffer中
        val actionsRdd: RDD[(String, CategoryCountInfo)] = data2Rdd.flatMap {
            action => {
                val actions: ListBuffer[(String, CategoryCountInfo)] = mutable.ListBuffer[(String, CategoryCountInfo)]()
                if (action.click_category_id != -1) {
                    actions.append((action.click_category_id.toString,
                        CategoryCountInfo(action.click_category_id.toString, 1L, 0L, 0L)))
                    actions
                } else if (action.order_category_ids != "null") {
                    val ids: Array[String] = action.order_category_ids.split(",")
                    for (id <- ids) actions.append((id, CategoryCountInfo(id, 0L, 1L, 0L)))
                    actions
                } else if (action.pay_category_ids != "null") {
                    val ids: Array[String] = action.pay_category_ids.split(",")
                    for (id <- ids) actions.append((id, CategoryCountInfo(id, 0L, 0L, 1L)))
                    actions
                } else Nil
            }
        }

        // 6. reduceByKey
        // Note: reduceByKey 参数为两个元素之间的逻辑关系函数，可参考reduceBy的底层源码，循环递归调用 op(x1, x2)
        val allActRdd: RDD[(String, CategoryCountInfo)] = actionsRdd.reduceByKey {
            (left, right) => {
                left.clickCount += right.clickCount
                left.orderCount += right.orderCount
                left.payCount += right.payCount
                left
            }
        }

        //7.  take top10

        // 采用sortBy的方法，以（click，order，pay）的逆序排列
        // 取前十ids
        val top10Category: Array[String] = allActRdd.sortBy(elem => (elem._2.clickCount, elem._2.orderCount, elem._2.payCount), false)
            .take(10)
            .map(_._2.categoryId)

        // 将top10的品类当作广播变量，来解决后续的需求
        val ids: Broadcast[Array[String]] = sc.broadcast(top10Category)

        // 8. top session in each id
        // 当一个用户产生一系列的session操作时，最初都是从click操作开始，因此统计session热度时，以click时的category_id记为该品类下的session
        // 下方的注释按照算子顺序依次解释
        // 8.1 首先过滤非top10的商品品类
        // 8.2 按照((category_id, session_id), 1L)的形式映射每一条数据
        // 8.3 采用reduceByKey()方法，统计同一品类下同一种session的总个数
        // 8.4 重新映射，((category_id, session_id), sum) => (category, (session_id, sum))
        // 8.5 以重新映射的key(category)为关键字，将同一品类下的所有session聚合在一起，形式为 (category, Iter((session_id, sum), ...))
        // 8.6 在此过程中，需要注意的是： 需求为每一个品类下的session热度排行，因此需要对每一个品类的值区域进行逻辑计算
        //     选择采用mapValues方法，仅操作值区域即Iter((session_id, sum), ...)),将其转化为list逆序排列，并取前10。
        // 8.7 加入行动算子，遍历打印输出

        data2Rdd.filter(action => ids.value.contains(action.click_category_id.toString))
            .map{ click => ((click.click_category_id.toString, click.session_id), 1L) }
            .reduceByKey(_ + _)
            .map{ session => (session._1._1, (session._1._2, session._2)) }
            .groupByKey()
            .mapValues{ data => data.toList.sortBy(_._2).reverse.take(10) }
            .foreach(println)

        // 9. top city in each id
        // 该Rdd的转换过程与8相似，仅将session_id替换为city_id，其余一致
        data2Rdd.filter(action => ids.value.contains(action.click_product_id.toString))
            .map( city => ((city.click_product_id.toString, city.city_id.toString), 1L))
            .reduceByKey(_ + _)
            .map{ city => (city._1._1, (city._1._2, city._2)) }
            .groupByKey()
            .mapValues{ city => city.toList.sortBy(_._2).reverse.take(10) }
            .foreach(println)


        // 10. conversion rate  页面单跳的转化率
        // 若某人的session访问过程为 1 -> 3 -> 2 -> 7 -> 10 -> 5，每次页面的跳转即为一次单跳
        // 页面单跳转化率为某一单跳在所有目标页面访问中的占比 如 1 -> 3的转化率为 按照1->3的顺序访问3页面的次数除以所有3页面的访问次数
        // 本例计算的页面单跳转化率为 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 过程中相邻页面的单跳转化率

        // 10.1 设置广播变量，可以减少Executor端频繁从Driver端读取list(1,2,3,4,5,6,7)
        val pages: Broadcast[ListBuffer[Int]] = sc.broadcast(ListBuffer(1,2,3,4,5,6,7))

        // 10.2 分母的计算，即某一页面的所有访问次数
        // 10.2.1 采用filter过滤出所有页面在 1 - 7 之间的页面， 可以有效的减少计算次数，开发中也是尽量先过滤不必要的数据后再进入逻辑计算
        // 10.2.2 映射为 (page_id, 1L)的形式
        // 10.2.3 采用reduceByKey，统计page_id出现的总次数，(page_id, sum)
        val denominator: RDD[(Long, Long)] = data2Rdd.filter(page => pages.value.contains(page.page_id))
            .map(data => (data.page_id, 1L))
            .reduceByKey(_ + _)

        // 10.3 分子的计算，即相邻页面的单跳次数
        // 10.3.1 以session_id为key进行聚合，(session_id, Iter(UserVisitAction, ...)
        // 10.3.2 在这一步直接使用了flatMap算子，因此data的转换过程较为复杂，下方按行依次解释
        //        首先，需要按照action的时间戳进行排序，从而保证页面之间的单跳为实际session中的单跳顺序，形式为 list(UserVisitAction, ...)
        //        将UserVisitAction类对象映射为所需要的page_id 形式为 list(page_id, ...)
        //        将list与list的tail进行拉链组合，得到相邻页面的组合 形式为 list((page_id1,page_id2), ...)
        //        可以在这一步进行过滤操作，去掉单跳组合中所有含有非1-7之间的页面及不相邻的元素
        //        flatMap中的元素需要为可迭代对象，因此将list((page_id1,page_id2), ...)中的每一个元素封装在一个Map中，
        //        并将该Map作为返回值  形式为 ((page_id1, page_id2), count)
        // 10.3.3 以reduceByKey的方法，计算各个相邻页面单跳的总次数 ((page_id1, page_id2), sum)

        val numerator: RDD[((Long, Long), Long)] = data2Rdd.groupBy(_.session_id)
            .flatMap {
                data =>
                    val actions: List[UserVisitAction] = data._2.toList.sortBy(_.action_time)
                    val pages1: List[Long] = actions.map(_.page_id)
                    val pagesToPages: List[(Long, Long)] = pages1.zip(pages1.tail)
                    val pagesToPages2: List[(Long, Long)] = pagesToPages.filter(
                        data => data._1 < 8 && data._2 < 8 && data._2 - data._1 == 1)
                    val data3: mutable.Map[(Long, Long), Long] = mutable.Map[(Long, Long), Long]()
                    for (elem <- pagesToPages2) data3(elem) = data3.getOrElse(elem, 0L) + 1
                    data3
            }
            .reduceByKey(_ + _)

        // 10.4 计算页面单跳转化率
        //      分别计算出分子和分母，并以 (page_id1, page_id2)中的sum除以page_id2的访问总次数
        val list1: List[(Long, Long)] = denominator.collect().toList
        val list2: List[((Long, Long), Long)] = numerator.collect().toList

        for (elem1 <- list1){
            for (elem2 <- list2) {
                if(elem1._1 == elem2._1._1)
                    println("page: " + elem2._1 + ", conversion rate: "  + (elem2._2 + 0.0) / elem1._2)
            }
        }
        
        // end: stop sc
        sc.stop()
    }
}
case class UserVisitAction(date: String, //用户点击行为的日期 
                           user_id: Long, //用户的ID 
                           session_id: String, //Session的ID 
                           page_id: Long, //某个页面的ID 
                           action_time: String, //动作的时间点 
                           search_keyword: String, //用户搜索的关键词 
                           click_category_id: Long, //某一个商品品类的ID 
                           click_product_id: Long, //某一个商品的ID 
                           order_category_ids: String, //一次订单中所有品类的ID集合 
                           order_product_ids: String, //一次订单中所有商品的ID集合 
                           pay_category_ids: String, //一次支付中所有品类的ID集合 
                           pay_product_ids: String, //一次支付中所有商品的ID集合 
                           city_id: Long) //城市 id 

case class CategoryCountInfo(var categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long) //支付次数