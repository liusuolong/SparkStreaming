package com.defdatasource_02

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**自定义数据源
 *
 * 1)	需求：自定义数据源，实现监控某个端口号，获取该端口号内容。
 * @author ymstart
 * @create 2020-10-30 20:40
 */
object SelfDefineDataSource {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建sparkStreaming上下文环境对象
    val ssc = new StreamingContext(conf,Seconds(3))

    //创建自定义receiver
    val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop002",9999))

    lineStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    ssc.awaitTermination()
  }
}

class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //创建一个Socket
 private var socket:Socket = _

  //读数据-->spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver"){
      setDaemon(true)
      override def run(): Unit = super.run()
    }.start()
  }

  def receive():Unit={
    try {
      socket = new Socket(host, port)
      //读取端口数据
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
      //接收数据
      var input:String = null
      //循环读取数据 并发送个spark
      while( (input=reader.readLine()) != null){
        store(input)
      }
    } catch {
      case  e :ConnectException=> restart(s"Error connecting  $host:$port",e)
        return
    }
  }

  override def onStop(): Unit = {
    if (socket != null){
      socket.close()
      socket = null
    }
  }
}