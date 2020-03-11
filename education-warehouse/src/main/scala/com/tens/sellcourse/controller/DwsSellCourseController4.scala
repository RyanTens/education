package com.tens.sellcourse.controller

import com.tens.sellcourse.service.DwsSellCourseService
import com.tens.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsSellCourseController4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dwd_sellcourse_import")//.setMaster("local[*]")
      .set("spark.sql.autoBroadcastJoinThreshold", "1")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    HiveUtil.useSnappyCompression(sparkSession)

    val dt = "20190722"
    DwsSellCourseService.importSellCourseDetail4(sparkSession, dt)
  }
}
