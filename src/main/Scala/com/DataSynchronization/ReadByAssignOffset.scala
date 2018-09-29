package com.DataSynchronization

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.KuduClient
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Auther fcvane
  * Date 2018/9/29
  */
class ReadByAssignOffset {

}

object ReadByAssignOffset extends App {
  /**
    * Kafka消费后的异常恢复
    * 从指定偏移量读取数据
    */
  val kd = KuduManager
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val ssc = new StreamingContext(conf, spark.streaming.Seconds(5))
  // 消费主题
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))
  val kuduClient = new KuduClient.KuduClientBuilder(properties.getProperty("kudu.master")).build()
  val topics = properties.getProperty("kafka.topic").split(",").toSet
  val kafkaBroker = properties.getProperty("kafka.broker")
  // 消费者配置
  val kafkaParams = Map[String, Object](
    // 用于初始化链接到集群的地址
    "bootstrap.servers" -> kafkaBroker,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    // 用于标识这个消费者属于哪个消费团体
    "group.id" -> "test"
  )
  // 参数
  if (args.length == 1) {
    // 读取offset
    val Array(offsetDirectory) = args
    val file = Source.fromFile(offsetDirectory)
    //  val offsetList = List((topics, 0, 1L), (topics, 1, 2L)) //指定topic，partition_no，offset
    val fromOffsets = collection.mutable.Map[TopicPartition, Long]()
    for (line <- file.getLines()) {
      val array = line.split(",")
      fromOffsets += (new TopicPartition(array(0), array(1).toInt) -> array(3).toLong)
    }
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )
    val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val total = ssc.sparkContext.longAccumulator("total")
    val tabName = new ArrayBuffer[String]()
    val currentTs = new ArrayBuffer[String]()

    // 数据处理
    kafkaStream.foreachRDD {
      (rdd, time) =>
        //      println(rdd, time, "------------------------", rdd.isEmpty())
        if (!rdd.isEmpty()) {
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          val starTime = timeFormat.format(new Date())
          // 处理数据
          rdd.foreachPartition {
            lines => {
              lines.foreach(line => {
                total.add(1)
                //              kf.dataParseJson(kuduClient, line.value())
                val tup = kd.kuduConnect(line.value())
                println(s"[ ReadByAssignOffset ] table and current_ts is : ${tup._2},${tup._3}")
                tabName += tup._2
                currentTs += tup._3
              })
            }
          }
          val rddTime = new Timestamp(time.milliseconds).toString.split("\\.")(0)
          val endTime = timeFormat.format(new Date())
          println(s"[ ReadByAssignOffset ] ${tabName} , ${currentTs}")
          println(s"[ ReadByAssignOffset ] synchronous base table name traversal: ${tabName.toList.distinct.mkString(",")}")
          println(s"[ ReadByAssignOffset ] current_ts data time: ${currentTs.toList.distinct.mkString(",")}")
          println(s"[ ReadByAssignOffset ] start writing file system ...")
          // 写HDFS
          //        LoggerManager.WHadoopDistributedFileSystem(starTime, endTime, rddTime, tabName, total.value)
          // 写本地文件系统
          LoggerManager.WLocalFileSystem(starTime, endTime, rddTime, tabName, total.value)
          println(s"[ ReadByAssignOffset ] write file system finished !")
          //清空数组
          tabName.clear()
          currentTs.clear()
          //恢复
          total.reset()
        }
        // 注释
        else {
          println(s"[ ReadByAssignOffset ] no data during this time period (${Seconds(5)})")
          println(s"[ ReadByAssignOffset ] start writing file system ...")
          //于HDFS上记录日志
          val nullTime = timeFormat.format(new Date())
          val rddTime = new Timestamp(time.milliseconds).toString.split("\\.")(0)
          // 切片没数据
          val nullName = new ArrayBuffer[String]()
          val total = 0
          // 写HDFS
          //         LoggerManager.WHadoopDistributedFileSystem(nullTime, nullTime, rddTime, nullName, total)
          // 写本地文件系统
          //        LoggerManager.WLocalFileSystem(nullTime, nullTime, rddTime, nullName, total)
          println(s"[ ReadByAssignOffset ] write file system finished !")
          // 清空数组
          tabName.clear()
          currentTs.clear()
        }
    }
  } else {
    System.err.println("[ ReadByAssignOffset ] 请输入Kafka offset的路径")
    System.exit(1)
  }

  ssc.start()
  ssc.awaitTermination()
}