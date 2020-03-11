package com.tens.member.controller

import com.tens.member.service.AdsMemberService
import com.tens.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AdsMemberController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ads_member_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession)

//    AdsMemberService.queryDetailSql(sparkSession,"20190722")
    AdsMemberService.queryDetailApi(sparkSession, "20190722")
  }
}
