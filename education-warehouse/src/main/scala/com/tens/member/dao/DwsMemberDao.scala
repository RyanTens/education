package com.tens.member.dao

import org.apache.spark.sql.SparkSession

object DwsMemberDao {
  def queryIdlMemberData(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |SELECT
         |uid ,
         |ad_id ,
         |memberlevel ,
         |register ,
         |appregurl ,
         |regsource ,
         |regsourcename ,
         |adname ,
         |siteid ,
         |sitename ,
         |vip_level ,
         |cast(paymoney as decimal(10,4)) as paymoney ,
         |dt ,
         |dn
         |FROM dws.dws_member
         |""".stripMargin)
  }

  def queryAppRegulCount(sparkSession: SparkSession, time: String) = {
    sparkSession.sql(
      s"""
         |
         |""".stripMargin)
  }

}
