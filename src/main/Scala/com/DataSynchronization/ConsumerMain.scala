package com.DataSynchronization

/**
  * Auther fcvane
  * Date 2018/8/30
  */

import java.io.{File, FileWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.KuduClient
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


class ConsumerMain {

}

object ConsumerMain extends App {

  val zk = ZookeeperManager
  val kf = KafkaManager
  val kd = KuduManager
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val ssc = new StreamingContext(conf, spark.streaming.Seconds(5))
  // spark参数信息(可在spark-submit执行命令行添加)
  conf.set("spark.default.parallelism", "500") //调整并发数
  //  conf.set("spark.streaming.stopGracefullyOnShutdown", "true") //优雅的关闭
  //  conf.set("spark.streaming.backpressure.enabled", "true") //激活削峰功能
  //  conf.set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值
  //  conf.set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
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
    "group.id" -> "test",
    // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    // 从最新的开始消费
    "auto.offset.reset" -> "latest",
    // 如果是true，则这个消费者的偏移量会在后台自动提交
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  // 参数
  // 读取offset
  // 采用zk 本地文件存储方式读取 -- kafka自身保存的不需要处理
  // 创建数据流
  var stream = kf.createDirectStream(ssc, kafkaParams, topics)
  if (args.length == 1) {
    println("[ ConsumerMain ] exists parameters: maybe zk or localfile")
    args(0) match {
      case "zk" =>
        println(s"[ ConsumerMain ] parameter is correct as ${args(0)}: zookeeper storage offsets")
        var stream = kf.createDirectStreamReadOffset(ssc, kafkaParams, topics, args(0))
      case "local" =>
        println(s"[ ConsumerMain ] parameter is correct as ${args(0)}: local file storage offsets")
        var stream = kf.createDirectStreamReadOffset(ssc, kafkaParams, topics, args(0))
      case _ =>
        println("[ ConsumerMain ] parameter error , will use kafka own storage")
        var stream = kf.createDirectStream(ssc, kafkaParams, topics)
    }
  }
  else {
    println("[ ConsumerMain ] process not exists parameters: kafka own storage")
    var stream = kf.createDirectStream(ssc, kafkaParams, topics)
  }

  val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val total = ssc.sparkContext.longAccumulator("total")
  val tabName = new ArrayBuffer[String]()
  val currentTs = new ArrayBuffer[String]()

  // 数据处理
  stream.foreachRDD {
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
              //              println(line.value(),"-------------------------")
              //              kf.dataParseJson(kuduClient, line.value())
              val tup = kd.kuduConnect(line.value())
              println(s"[ ConsumerMain ] table and current_ts is : ${tup._2},${tup._3}")
              tabName += tup._2
              currentTs += tup._3
            })
          }
        }
        val rddTime = new Timestamp(time.milliseconds).toString.split("\\.")(0)
        val endTime = timeFormat.format(new Date())
        println(s"[ ConsumerMain ] " + tabName.mkString(","), currentTs.mkString(","))
        println(s"[ ConsumerMain ] synchronous base table name traversal: ${tabName.toList.distinct.mkString(",")}")
        println(s"[ ConsumerMain ] current_ts data time: ${currentTs.toList.distinct.mkString(",")}")
        println(s"[ ConsumerMain ] start writing file system ...")
        // 写HDFS
        //        LoggerManager.WHadoopDistributedFileSystem(starTime, endTime, rddTime, tabName, total.value)
        // 写本地文件系统
        LoggerManager.WLocalFileSystem(starTime, endTime, rddTime, tabName, total.value)
        println(s"[ ConsumerMain ] write file system finished !")
        //清空数组
        tabName.clear()
        currentTs.clear()
        //恢复
        total.reset()

        // 保存偏移量
        // zk存储偏移量
        for (i <- 0 until offsetRanges.length) {
          println(s"[ ConsumerMain ] topic: ${offsetRanges(i).topic}; partition: ${offsetRanges(i).partition}; " +
            s"fromoffset: ${offsetRanges(i).fromOffset}; utiloffset: ${offsetRanges(i).untilOffset}")
          // 写zookeeper
          zk.zkSaveOffset(s"${offsetRanges(i).topic}offset", s"${offsetRanges(i).partition}",
            s"${offsetRanges(i).topic},${offsetRanges(i).partition},${offsetRanges(i).fromOffset},${offsetRanges(i).untilOffset}")
        }
        // 关闭
        zk.zooKeeper.close()
        // 写本地文件系统 -清空
        val fwClear = new FileWriter(new File("./files/offset.log"), false)
        fwClear.write("")
        fwClear.flush()
        fwClear.close()
        // 写本地文件系统 -追加
        val fwAppend = new FileWriter(new File("./files/offset.log"), true)
        for (i <- 0 until offsetRanges.length) {
          fwAppend.write(offsetRanges(i).topic + "," + offsetRanges(i).partition + "," + offsetRanges(i).fromOffset + "," + offsetRanges(i).untilOffset + "\n")
        }
        fwAppend.close()
        // 新版本Kafka自身保存
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
      // 注释
      else {
        println(s"[ ConsumerMain ] no data during this time period (${Seconds(5)})")
        println(s"[ ConsumerMain ] start writing file system ...")
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
        println(s"[ ConsumerMain ] write file system finished !")
        // 清空数组
        //        tabName.clear()
        //        currentTs.clear()
      }
  }
  ssc.start()
  ssc.awaitTermination()
}