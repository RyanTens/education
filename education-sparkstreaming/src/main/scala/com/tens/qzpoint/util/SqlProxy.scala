package com.tens.qzpoint.util

import java.sql.{Connection, PreparedStatement, ResultSet}

trait QueryCallback{
  def process(rs: ResultSet)
}
class SqlProxy {
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]) ={
    var rtn = 0
    try {
      psmt = conn.prepareStatement(sql)
      if(params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rtn = psmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  def shutdown(conn: Connection) = {
    DataSourceUtil.closeResource(rs, psmt, conn)
  }

  private var rs: ResultSet = _
  private var psmt: PreparedStatement = _

  /**
   * 执行查询语句
   * @param conn
   * @param sql
   * @param params
   * @param queryCallback
   */
  def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallback: QueryCallback) = {
    rs = null
    try {
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rs = psmt.executeQuery()
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }




}
