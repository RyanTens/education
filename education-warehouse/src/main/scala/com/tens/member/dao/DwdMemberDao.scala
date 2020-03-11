package com.tens.member.dao

import org.apache.spark.sql.SparkSession

object DwdMemberDao {
  def getDwdVipLevel(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |SELECT
         |vip_id ,
         |vip_level ,
         |start_time as vip_start_time,
         |end_time as vip_end_time,
         |last_modify_time as vip_last_modify_time,
         |max_free as vip_max_free,
         |min_free as vip_min_free,
         |next_level as vip_next_level,
         |operator as vip_operator,
         |dn
         |FROM dwd.dwd_vip_level
         |""".stripMargin)
  }

  def getDwdPcentermemPaymoney(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |SELECT
         |uid ,
         |CAST (paymoney as decimal(10,4)) as paymoney ,
         |vip_id ,
         |dn
         |FROM dwd.dwd_pcentermempaymoney
         |""".stripMargin)
  }

  def getDwdBaseWebsite(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |SELECT
         |siteid ,
         |sitename ,
         |siteurl ,
         |`delete` as site_delete ,
         |createtime as site_createtime ,
         |creator as site_creator ,
         |dn
         |FROM dwd.dwd_base_website
         |""".stripMargin)
  }

  def getDwdBaseAd(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |SELECT
         |adid as ad_id,
         |adname ,
         |dn
         |FROM dwd.dwd_base_ad
         |""".stripMargin)
  }

  def getDwdMemberRegtype(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |SELECT
         |uid ,
         |appkey ,
         |appregurl,
         |bdp_uuid ,
         |createtime as reg_createtime ,
         |`domain` ,
         |isranreg ,
         |regsource ,
         |regsourcename ,
         |websiteid as siteid ,
         |dn
         |FROM dwd.dwd_member_regtype
         |""".stripMargin)
  }

  def getDwdMember(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |SELECT
         |uid ,
         |ad_id ,
         |email ,
         |fullname ,
         |iconurl ,
         |lastlogin ,
         |mailaddr ,
         |memberlevel ,
         |password ,
         |phone ,
         |qq ,
         |register ,
         |regupdatetime ,
         |unitname ,
         |userip ,
         |zipcode ,
         |dt ,
         |dn
         |FROM dwd.dwd_member
         |""".stripMargin)
  }

}
