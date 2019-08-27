package com.DataSynchronization

/**
  * Auther fcvane
  * Date 2018/8/30
  */

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import org.apache.kudu.Type
import org.apache.kudu.client.KuduClient


class KuduManager {

}

object KuduManager {
  /**
    * Kudu操作
    */


  // PROPERTIES文件读取
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))

  /**
    *
    * @param line 文件流数据
    * @return
    */

  def kuduConnect(line: String): (String, String, String) = {
    // 创建连接
    val kuduClient = new KuduClient.KuduClientBuilder(properties.getProperty("kudu.master")).build()
    val newSession = kuduClient.newSession()
    var primaryKey: String = null
    var tabName: String = null
    var currentTs: String = null
    println("[ KuduManager ] kudu connect")
    // 当前时间
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    try {
      // 解析json
      val json = JSON.parseObject(line)
      // 返回字符串成员
      val tableName = json.getString("table")
      val data = json.getJSONObject("after")
      // 主键获取 (所有主键名都为ID)
      val primaryKey = data.get("ID").toString
      val opType = json.getString("op_type")
      // 匹配判断表是否存在
      tableName match {
        case null =>
          println("[ KuduManager ] json is abnormal , not exists tablename")
          throw new RuntimeException
        case _ =>
          println("[ KuduManager ] json is normal")
          currentTs = json.getString("current_ts")
          // Kafka接收OGG数据 表名为:Schema.tabname(注意大小写)
          tabName = json.getString("table").split("\\.")(1).toLowerCase()
          // 事件表调用
          eventConnect(kuduClient, primaryKey, tabName, currentTs, date)
          // 基表处理
          val kuduTable = kuduClient.openTable(json.getString("table").split("\\.")(1).toLowerCase())
          val schema = kuduTable.getSchema
          val upsert = kuduTable.newUpsert
          val row = upsert.getRow
          // 提前处理删除状态的数据，避免重复处理无用的删除记录 --fcvane 20190827
          // 状态 delete_state 字段赋值:物理删除改逻辑删除
          opType.toString match {
            case "D" =>
              row.addString("delete_state", "1")
            case _ =>
              row.addString("delete_state", "0")
              // 字段赋值
              for (i <- 0 until schema.getColumns.size()) {
                val colSchema = schema.getColumnByIndex(i)
                val colName = colSchema.getName.toUpperCase
                //            println("[ KuduManager ] " + data.get(colName))
                val colType: Type = colSchema.getType
                if (data.get(colName) != null) {
                  // 添加其他类型操作
                  colType match {
                    //BINARY
                    case Type.BINARY =>
                      row.addString(colName.toLowerCase(), data.get(colName).toString)
                    // 字符
                    case Type.STRING =>
                      row.addString(colName.toLowerCase(), data.get(colName).toString)
                    // BOOL
                    case Type.BOOL =>
                      row.addByte(colName.toLowerCase(), data.get(colName).toString.toByte)
                    // Double
                    case Type.DOUBLE =>
                      row.addDouble(colName.toLowerCase(), data.get(colName).toString.toDouble)
                    // Float
                    case Type.FLOAT =>
                      row.addFloat(colName.toLowerCase(), data.get(colName).toString.toFloat)
                    // 整型 && UNIXTIME_MICROS
                    case _ =>
                      row.addInt(colName.toLowerCase(), data.get(colName).toString.toInt)
                  }
                }
              }
          }
          // 同步处理时间
          row.addString("time_stamp", date)
          newSession.apply(upsert)
          kuduClient.close()
      }
    } catch {
      case ex: NullPointerException =>
        println("[ KuduManager ] no tablename is found")
      case ex: com.alibaba.fastjson.JSONException =>
        println("[ KuduManager ] Kafka infomation format is incorrect")
    }
    (primaryKey, tabName, currentTs)
  }

  /**
    * 事件表处理
    *
    * @param primaryKey 基表主键ID
    * @param tabName    基表名称
    * @param currentTs  基表同步时间
    * @return 执行结果入库 不需要返回值
    */
  def eventConnect(kuduClient: KuduClient, primaryKey: String, tabName: String, currentTs: String, date: String) {
    println("[ KuduManager ] event table processing")
    val newSession = kuduClient.newSession()
    val eventTable = kuduClient.openTable("pub_event")
    val eventSchema = eventTable.getSchema
    val eventUpsert = eventTable.newUpsert
    val eventRow = eventUpsert.getRow
    if (primaryKey != null && tabName != null) {
      // 主键
      eventRow.addString("id", primaryKey)
      // 基表名
      eventRow.addString("name", tabName)
      // OGG同步时间
      eventRow.addString("current_ts", currentTs.split("T").mkString(" "))
      // 初始化状态时
      // 预留字段
      eventRow.addString("delete_state", "0")
      eventRow.addString("his_delete_state", "0")
      // 同步处理时间
      eventRow.addString("time_stamp", date)
      // 提交
      newSession.apply(eventUpsert)
    }
    println("[ KuduManager ] event table finish")
  }
}

