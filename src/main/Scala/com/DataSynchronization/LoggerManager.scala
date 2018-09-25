package com.DataSynchronization

import java.io.{File, FileWriter}
import java.sql.Timestamp
import java.util.Date

import com.DataSynchronization.ConsumerMain.dateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * Auther fcvane
  * Date 2018/9/25
  */
class LoggerManager {

}

object LoggerManager {
  /**
    *
    * 日志管理
    */

  /**
    * 日志的本地文件存储
    *
    * @param starTime 批次开始时间
    * @param endTime  批次结束时间
    * @param rddTime  RDD处理时间
    * @param tabName  同步的基表名称
    * @param total    同步的基表总数
    */
  def WLocalFileSystem(starTime: String, endTime: String, rddTime: String, tabName: ArrayBuffer[String], total: Long) {
    val logfile = "./files/tbLog" + dateFormat.format(new Date()) + ".log"
    val fw = new FileWriter(new File(logfile), true)
    val diff: Double = (Timestamp.valueOf(endTime).getTime - Timestamp.valueOf(starTime).getTime) / (1000)
    var rate: String = null
    if (diff == 0) {
      rate = total.toString
    } else {
      val r: Double = total.toDouble / diff
      rate = r.formatted("%.4f")
    }
    // 写本地文件系统
    fw.write("\n"
      + "基表同步启动时间: " + starTime + "\n"
      + "基表同步结束时间: " + endTime + "\n"
      + "基表增量名称遍历: " + tabName.toList.distinct.mkString(",") + "\n"
      + "增量数据同步时间: " + rddTime + "\n"
      + "增量过程同步总数: " + total + "\n"
      + "基表同步数据效率: " + rate + " rec/s" + "\n"
    )
    // 关闭文件流
    fw.close()
  }

  /**
    * 日志的hdfs存储
    *
    * @param starTime 批次开始时间
    * @param endTime  批次结束时间
    * @param rddTime  RDD处理时间
    * @param tabName  同步的基表名称
    * @param total    同步的基表总数
    */
  def WHadoopDistributedFileSystem(starTime: String, endTime: String, rddTime: String, tabName: ArrayBuffer[String], total: Long) {
    val logfile = "/tmp/topics/tbLog" + dateFormat.format(new Date()) + ".log"
    val configuration = new Configuration()
    val configPath = new Path(logfile)
    val fileSystem: FileSystem = configPath.getFileSystem(configuration)
    var fsw: FSDataOutputStream = null;
    if (!fileSystem.exists(configPath)) {
      fsw = fileSystem.create(configPath)
    } else {
      fsw = fileSystem.append(configPath)
    }
    val diff: Double = (Timestamp.valueOf(endTime).getTime - Timestamp.valueOf(starTime).getTime) / (1000)
    var rate: String = null
    if (diff == 0) {
      rate = total.toString
    } else {
      val r: Double = total.toDouble / diff
      rate = r.formatted("%.4f")
    }
    fsw.write(("\n"
      + "基表同步启动时间: " + starTime + "\n"
      + "基表同步结束时间: " + endTime + "\n"
      + "基表增量名称遍历: " + tabName.toList.distinct.mkString(",") + "\n"
      + "增量数据同步时间: " + rddTime + "\n"
      + "增量过程同步总数: " + total + "\n"
      + "基表同步数据效率: " + rate + " rec/s" + "\n"
      //          + "基表写入失败总数: " + errorTotal.value + "\n"
      ).getBytes("UTF-8"))
    fsw.close()
    fileSystem.close()
  }

  def main(args: Array[String]): Unit = {

  }
}
