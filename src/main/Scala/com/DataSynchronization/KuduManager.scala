package com.DataSynchronization

/**
  * Auther fcvane
  * Date 2018/8/30
  */
import java.util.Properties

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
  val kuduClient = new KuduClient.KuduClientBuilder(properties.getProperty("kudu.master")).build()
  val newSession = kuduClient.newSession()


  def kuduConnect(line: String): (String, String) = {
    var btName: String = null
    var currentTs: String = null
    println("[ KuduManager ] kudu connect")
    //解析json
    val json = JSON.parseObject(line)
    //返回字符串成员
    val tableName = json.getString("table")
    val data = json.getJSONObject("after")
    val opType = json.getString("op_type")
    //匹配判断表是否存在
    tableName match {
      case null =>
        println("[ KuduManager ] json is abnormal where not exists tablename ")
        throw new RuntimeException
      case _ =>
        println("[ KuduManager ] json is normal ")
        btName = tableName
        currentTs = json.getString("current_ts")
        val kuduTable = kuduClient.openTable(json.getString("table").split("\\.")(1).toLowerCase())
        val schema = kuduTable.getSchema
        val upsert = kuduTable.newUpsert
        val row = upsert.getRow

        // 字段赋值
        for (i <- 0 until schema.getColumns.size()) {
          val colSchema = schema.getColumnByIndex(i)
          val colName = colSchema.getName.toUpperCase
          println("[ KuduManager ] " + data.get(colName))
          val colType: Type = colSchema.getType
          if (data.get(colName) != null) {
            colType match {
              case Type.STRING =>
                row.addString(colName.toLowerCase(), data.get(colName).toString)
              case _ =>
                row.addInt(colName.toLowerCase(), data.get(colName).toString.toInt)
            }
          }
        }
        // 状态 delete_state 字段赋值
        opType.toString match {
          case "D" =>
            row.addString("delete_state", "1")
          case _ =>
            row.addString("delete_state", "0")
        }
        newSession.apply(upsert)
//        kuduClient.close()
    }
    (btName, currentTs)

  }
}
