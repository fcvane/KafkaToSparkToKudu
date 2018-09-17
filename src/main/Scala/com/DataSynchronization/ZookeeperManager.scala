package com.DataSynchronization

/**
  * Auther fcvane
  * Date 2018/8/30
  */

import java.util.Properties

import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}


class ZookeeperManager {

}

object ZookeeperManager {
  /**
    * Zookeeper管理
    */

  val TIME_OUT = 5000
  var zooKeeper: ZooKeeper = _

  def watcher = new Watcher() {
    def process(event: WatchedEvent) {
      println("[ ZookeeperManager ] already triggered " + event.getType + " event !")
    }
  }

  // PROPERTIES文件读取
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))

  /**
    * 基础方法
    */
  def connect() {
    println(s"[ ZookeeperManager ] zk connect")
    zooKeeper = new ZooKeeper(properties.getProperty("zookeeper.quorm"), TIME_OUT, watcher)
  }

  /**
    * 创建znode
    *
    * @param znode 数据节点
    * @param data  节点数据
    * @return zk连接
    */
  def znodeCreate(znode: String, data: String) {
    println(s"[ ZookeeperManager ] zk create /$znode , $data")
    //    if zooKeeper.
    zooKeeper.create(s"/$znode", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  /**
    * 更新znode
    *
    * @param znode 数据节点
    * @param data  节点数据
    * @return 节点数据更新
    */
  def znodeDataSet(znode: String, data: String) {
    println(s"[ ZookeeperManager ] zk data set /$znode")
    zooKeeper.setData(s"/$znode", data.getBytes(), -1)
  }

  /**
    * 获取znode数据
    *
    * @param znode 数据节点
    * @return Map 返回Map
    **/
  def znodeDataGet(znode: String): Map[TopicPartition, Long] = {
    //: Map[TopicPartition, Long] =
    connect()
    println(s"[ ZookeeperManager ] zk data get /$znode")
    val children = zooKeeper.getChildren(s"/$znode", true).toArray()
    if (children.length > 0) {
      val fromOffsets = children.map(x => new String(zooKeeper.getData(s"/$znode/$x", true, null), "utf-8").split(","))
        .map(x => new TopicPartition(x(0), x(1).toInt) -> x(3).toLong).toMap
      println(s"[ ZookeeperManager ] zk data : ${fromOffsets}")
      fromOffsets
    }
    else {
      val fromOffsets = Set(new String(zooKeeper.getData(s"/$znode", true, null), "utf-8").split(",")).map { x =>
        new TopicPartition(x(0), x(1).toInt) -> x(3).toLong
      }.toMap
      println(s"[ ZookeeperManager ] zk data : ${fromOffsets}")
      fromOffsets
    }
  }

  /**
    * 保存每个批次的rdd的offset到zk中
    *
    * @param znode     数据节点
    * @param partition Kafka分区
    * @param  data     节点数据 格式: 主题1,分区序号1,变化前的偏移量1,变化后的偏移量1;主题2,分区序号2,变化前的偏移量2,变化后的偏移量2,......
    * @return 节点数据获取
    */
  def zkSaveOffset(znode: String, partition: String, data: String) {
    connect()
    println(s"[ ZookeeperManager ] offset work /$znode")
    if (partition != null) {
      zooKeeper.exists(s"/$znode/$partition", true) match {
        case null => {
          zooKeeper.exists(s"/$znode", true) match {
            case null =>
              println(s"[ ZookeeperManager ] /$znode is not exists ")
              znodeCreate(s"$znode", "offset")
              znodeCreate(s"$znode/$partition", data)
            case _ => println(s"[ ZookeeperManager ] /$znode is exists ")
              zooKeeper.exists(s"/$znode/$partition", true) match {
                case null =>
                  //会话失效或者连接丢失后的重新生成新的session
                  try {
                    znodeCreate(s"$znode/$partition", data)
                  }
                  catch {
                    case _: Exception =>
                      zooKeeper = new ZooKeeper(properties.getProperty("zookeeper.quorm"), TIME_OUT, watcher)
                      znodeCreate(s"$znode/$partition", data)
                  }
                case _ =>
                  znodeDataSet(s"$znode/$partition", data)
              }
          }
        }
        case _ => znodeDataSet(s"$znode/$partition", data)
      }
    }
    else {
      zooKeeper.exists(s"/$znode", true) match {
        case null =>
          println(s"[ ZookeeperManager ] /$znode is not exists ")
          znodeCreate(s"$znode", data)
        case _ =>
          println(s"[ ZookeeperManager ] /$znode is exists ")
          znodeDataSet(s"$znode", data)
      }
    }
    zooKeeper.close()
  }

  def main(args: Array[String]): Unit = {
    znodeDataGet("oggoffset")
    //    znodeDataGet("ogg")

  }
}