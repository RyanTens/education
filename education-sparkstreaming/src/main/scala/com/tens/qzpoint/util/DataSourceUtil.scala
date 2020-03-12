package com.tens.qzpoint.util

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.log4j.Logger


object DataSourceUtil {
  private val log = Logger.getLogger(DataSourceUtil.getClass.getName)

  val dataSource = {
    try {
      val props = new Properties()
      props.setProperty("url", ConfigurationManager.getProperty("jdbc.url"))
      props.setProperty("username", ConfigurationManager.getProperty("jdbc.user"))
      props.setProperty("password", ConfigurationManager.getProperty("jdbc.password"))
      props.setProperty("initialSize", "5") //初始化大小
      props.setProperty("maxActive", "10") //最大连接
      props.setProperty("minIdle", "5") //最小连接
      props.setProperty("maxWait", "60000") //等待时长
      props.setProperty("timeBetweenEvictionRunsMillis", "2000") //配置多久进行一次检测,检测需要关闭的连接 单位毫秒
      props.setProperty("minEvictableIdleTimeMillis", "600000") //配置连接在连接池中最小生存时间 单位毫秒
      props.setProperty("maxEvictableIdleTimeMillis", "900000") //配置连接在连接池中最大生存时间 单位毫秒
      props.setProperty("validationQuery", "select 1")
      props.setProperty("testWhileIdle", "true")
      props.setProperty("testOnBorrow", "false")
      props.setProperty("testOnReturn", "false")
      props.setProperty("keepAlive", "true")
      props.setProperty("phyMaxUseCount", "100000")
      Some(DruidDataSourceFactory.createDataSource(props))
    } catch {
      case error: Error =>
        log.error("Error Create Mysql Connection", error)
        None
    }
  }

  //提供获取连接的方法
  def getConnection = {
    dataSource match {
      case Some(ds) => Some(ds.getConnection())
      case None => None

    }

  }

  //关闭资源
  def closeResource(resultSet: ResultSet, preparedStatement: PreparedStatement, connection: Connection) = {
    if (resultSet != null) {
      try {
        resultSet.close()
      } catch {
        case error: Error =>
          log.error("Error ResultSet close", error)
      }
    }

    if (preparedStatement != null) {
      try {
        preparedStatement.close()
      } catch {
        case error: Error =>
          log.error("Error PreparedStatement close", error)
      }
    }

    if (connection != null) {
      try {
        connection.close()
      } catch {
        case error: Error =>
          log.error("Error Connection close", error)
      }
    }
  }
}
