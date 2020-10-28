package com.ym123.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ymstart
 * @create 2020-09-23 14:51
 */
object Test08_Sample {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,4,6,8,10,22,44))
    /**
     * 伯努利算法
     * 不放回
     * 参数解读：
     * withReplacement:
     *                false 不放回 true 放回
     * fraction: 样本的预期大小，占该RDD大小的一部分*无需替换：
     *          选择每个元素的概率；分数必须为[0，1] *并带有替换：
     *          选择每个元素的预期次数；小数必须大于*等于0
     * seed:随机数生成器的种子 种子数相同则 生成的随机数相同
     */
    //rdd.sample(false,0.5,6).collect().foreach(println)


    /**
     * 泊松算法 放回
     *
     * withReplacement:
     *                 false 不放回 true 放回
     * fraction:[0,+无穷大
     *
     * seed:随机数生成器的种子 种子数相同则 生成的随机数相同
     */
    rdd.sample(true,1.5,10).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
