package com.tens.member.controller

import com.tens.member.service.DwsMemberService
import com.tens.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsMemberController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dws_member_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    HiveUtil.useSnappyCompression(sparkSession)

    DwsMemberService.importMember(sparkSession, "20190722") //根据用户信息聚合用户表数据
//    DwsMemberService.importMemberApi(sparkSession, "20190722") //api方式实现
  }
}
