package com.tens.member.service

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object EtlDataService {
  def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/tens/ods/pcenterMemViplevel.log")
      .filter(JSON.parseObject(_).isInstanceOf[JSONObject])
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObj: JSONObject = JSON.parseObject(item)
          val vip_id = jsonObj.getIntValue("vip_id")
          val vip_level = jsonObj.getString("vip_level")
          val start_time = jsonObj.getString("start_time")
          val end_time = jsonObj.getString("end_time")
          val last_time = jsonObj.getString("last_time")
          val max_free = jsonObj.getString("max_free")
          val min_free = jsonObj.getString("min_free")
          val next_level = jsonObj.getString("next_level")
          val operator = jsonObj.getString("operator")
          val dn = jsonObj.getString("dn")
          (vip_id, vip_level, start_time, end_time, last_time, max_free, min_free, next_level, operator, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
  }

  def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/tens/ods/pcentermempaymoney.log")
      .filter(JSON.parseObject(_).isInstanceOf[JSONObject])
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObj: JSONObject = JSON.parseObject(item)
          val uid = jsonObj.getIntValue("uid")
          val paymoney = jsonObj.getString("paymoney")
          val siteid = jsonObj.getIntValue("siteid")
          val vip_id = jsonObj.getIntValue("vip_id")
          val dt = jsonObj.getString("dt")
          val dn = jsonObj.getString("dn")
          (uid, paymoney, siteid, vip_id, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_pcentermempaymoney")
  }

  /**
   * etl用户信息
   * @param ssc
   * @param sparkSession
   */
  def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/tens/ods/memberRegtype.log")
      .filter(JSON.parseObject(_).isInstanceOf[JSONObject])
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObj = JSON.parseObject(item)
          val appkey = jsonObj.getString("appkey")
          val appregurl = jsonObj.getString("appregurl")
          val bdp_uuid = jsonObj.getString("bdp_uuid")
          val createtime = jsonObj.getString("createtime")
          val dn = jsonObj.getString("dn")
          val domain = jsonObj.getString("domain")
          val dt = jsonObj.getString("dt")
          val isranreg = jsonObj.getString("isranreg")
          val regsource = jsonObj.getString("regsource")
          val regsourceName = regsource match {
            case "1" => "PC"
            case "2" => "Mobile"
            case "3" => "App"
            case "4" => "WeChat"
            case _ => "other"
          }
          val uid = jsonObj.getIntValue("uid")
          val websiteid = jsonObj.getIntValue("websiteid")
          (uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid,
            dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_member_regtype")
  }

  /**
   * etl用户表数据
   * @param ssc
   * @param sparkSession
   */
  def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/tens/ods/member.log")
      .filter(JSON.parseObject(_).isInstanceOf[JSONObject])
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObj = JSON.parseObject(item)
          val ad_id = jsonObj.getIntValue("ad_id")
          val birthday = jsonObj.getString("birthday")
          val dn = jsonObj.getString("dn")
          val dt = jsonObj.getIntValue("dt")
          val email = jsonObj.getString("email")
          val fullname = jsonObj.getString("fullname").substring(0,1) + "XX"
          val iconurl = jsonObj.getString("iconurl")
          val lastlogin = jsonObj.getString("lastlogin")
          val mailaddr = jsonObj.getString("mailaddr")
          val memberlevel = jsonObj.getString("memberlevel")
          val password = "******"
          val paymoney = jsonObj.getString("paymoney")
          val phone = jsonObj.getString("phone").replaceAll("(\\d{3})(\\d{4})(\\d{4})", "$1****$3")
          val qq = jsonObj.getString("qq")
          val register = jsonObj.getString("register")
          val regupdatetime = jsonObj.getString("regupdatetime")
          val uid = jsonObj.getIntValue("uid")
          val unitname = jsonObj.getString("unitname")
          val userip = jsonObj.getString("userip")
          val zipcode = jsonObj.getString("zipcode")
          (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, phone,
            qq, register, regupdatetime, unitname, userip, zipcode, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_member")
  }

  /**
   * 导入网站表基础数据
   * @param ssc
   * @param sparkSession
   */
  def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //导入隐式转换
    ssc.textFile("/user/tens/ods/baswewebsite.log")
      .filter(JSON.parseObject(_).isInstanceOf[JSONObject])
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObj: JSONObject = JSON.parseObject(item)
          val createtime = jsonObj.getString("createtime")
          val creator = jsonObj.getString("creator")
          val delete = jsonObj.getIntValue("delete")
          val dn = jsonObj.getString("dn")
          val siteid = jsonObj.getIntValue("siteid")
          val sitename = jsonObj.getString("sitename")
          val siteurl = jsonObj.getString("siteurl")
          (siteid, sitename, siteurl, delete, createtime, creator, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }

  /**
   * 导入广告表基础数据
   * @param ssc
   * @param sparkSession
   */
  def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //导入隐式转换
    ssc.textFile("/user/tens/ods/baseadlog.log")
      .filter(JSON.parseObject(_).isInstanceOf[JSONObject])
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObj: JSONObject = JSON.parseObject(item)
          val adid = jsonObj.getIntValue("adid")
          val adname = jsonObj.getString("adname")
          val dn = jsonObj.getString("dn")
          (adid, adname, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
  }

}
