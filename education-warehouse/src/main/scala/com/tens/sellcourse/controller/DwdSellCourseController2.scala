package com.tens.sellcourse.controller

import com.tens.sellcourse.service.DwdSellCourseService
import com.tens.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdSellCourseController2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dwd_sellcourse_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder()
//      .config("spark.sql.parquet.writeLegacyFormat", true)
//      .config("spark.sql.hive.convertMetastoreParquet", false)
      .config(conf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
//    HiveUtil.useSnappyCompression(sparkSession)
//    DwdSellCourseService.importSaleCourseLog(ssc, sparkSession)
//    DwdSellCourseService.importCoursePay2(ssc, sparkSession)
    DwdSellCourseService.importCourseShoppingCart2(ssc, sparkSession)

  }
}
