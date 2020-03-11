package com.tens.education.qz.test

import com.tens.qz.service.AdsQzService
import com.tens.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object TestController {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ads_qz_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession)
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    val dt = "20190722"
    AdsQzService.getTargetApi(sparkSession, dt)
  }
}
