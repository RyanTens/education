package com.tens.qz.dao

import org.apache.spark.sql.SparkSession

object QzMajorDao {
  def getQzMajor(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |majorid,
         |businessid,
         |siteid,
         |majorname,
         |shortname,
         |status,
         |sequence,
         |creator as major_creator,
         |createtime as major_createtime,
         |dt,
         |dn
         |from dwd.dwd_qz_major
         |where dt='$dt'
         |""".stripMargin)
  }

  def getQzWebsite(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |siteid,
         |sitename,
         |domain,
         |multicastserver,
         |templateserver,
         |creator,
         |createtime,
         |multicastgateway,
         |multicastport,dn
         |from dwd.dwd_qz_website
         |where dt='$dt'
         |""".stripMargin)
  }

  def getQzBusiness(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |businessid,
         |businessname,
         |dn
         |from dwd.dwd_qz_business
         |where dt='$dt'
         |""".stripMargin)
  }
}
