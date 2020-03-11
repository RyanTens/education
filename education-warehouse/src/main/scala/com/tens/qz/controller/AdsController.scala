package com.tens.qz.controller

import com.tens.qz.service.AdsQzService
import com.tens.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdsController {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ads_qz_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    //HiveUtil.openCompression(sparkSession) //开启压缩
    //HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    val dt = "20190722"
//    AdsQzService.getTarget(sparkSession, dt) //sql实现指标
    AdsQzService.getTargetApi(sparkSession,dt) //api实现指标
  }
}
