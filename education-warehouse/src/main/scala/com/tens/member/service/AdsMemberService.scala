package com.tens.member.service

import com.tens.member.bean.QueryResult
import com.tens.member.dao.DwsMemberDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object AdsMemberService {

  /**
    * 统计各项指标 使用api
    * @param sparkSession
    * @param time
    */
  def queryDetailApi(sparkSession: SparkSession, time: String) = {
    //导入隐式转换
    import sparkSession.implicits._
    val result = DwsMemberDao
      .queryIdlMemberData(sparkSession)
      .as[QueryResult]
      .where(s"dt='$time'")
    result.cache()
    //统计注册来源url人数
    result.mapPartitions(partition => {
      partition.map(item => (item.appregurl + "_" + item.dn + "_" + item.dt , 1))
      //(www.xxx.com_webA_20190722,1),(www.xxx.com_webA_20190722,1)......
    }).groupByKey(_._1)
    //((www.aaa.com_webA_20190722,1),Iterator[((www.aaa.com_webA_20190722,1),(www.aaa.com_webA_20190722,1)...)])
      .mapValues(_._2)
      .reduceGroups(_ + _) //(www.aaa.com_webA_20190722,3)
      .map(item => {
        val keys = item._1.split("_")
        val appregurl = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (appregurl, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_appregurlnum")

    //统计所属网站人数
    result.mapPartitions(partition => {
      partition.map(item => (item.sitename + "_" + item.dn + "_" + item.dt , 1))
    }).groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val sitename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (sitename, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_sitenamenum")

    //统计所属来源人数 pc mobile wechat app
    result.mapPartitions(partition => {
      partition.map(item => (item.regsourcename + "_" + item.dn + "_" + item.dt , 1))
    }).groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (regsourcename, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_regsourcenamenum")

    //统计通过各广告进来的人数
    result.mapPartitions(partition => {
      partition.map(item => (item.adname + "_" + item.dn + "_" + item.dt , 1))
    }).groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val adname = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (adname, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_adnamenum")

    //统计各等级用户人数
    result.mapPartitions(partition => {
      partition.map(item => (item.memberlevel + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val memberlevel = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (memberlevel, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_memberlevelnum")

    //统计各vip等级人数
    result.mapPartitions(partition => {
      partition.map(item => (item.vip_level + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val vip_level = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (vip_level, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_viplevelnum")

    //统计各memberlevel等级 支付金额前三的用户
    import org.apache.spark.sql.functions._ //withColumn,row_number等方法需要导入此包
    result
      .withColumn(
        "rownum",
        row_number().over(
          Window.partitionBy("dn", "memberlevel").orderBy(desc("paymoney"))
        )
      )
      .where("rownum < 4")
      .orderBy("memberlevel", "rownum")
      .select(
        "uid",
        "memberlevel",
        "register",
        "appregurl",
        "regsourcename",
        "adname",
        "sitename",
        "vip_level",
        "paymoney",
        "rownum",
        "dt",
        "dn"
      ).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_top3memberpay")
  }

  /**
    * 使用sql统计各项指标
    *
    * @param sparkSession
    * @param time
    */
  def queryDetailSql(sparkSession: SparkSession, time: String) = {
    DwsMemberDao.queryAppRegulCount(sparkSession, time)
  }

}
