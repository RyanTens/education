package com.tens.sellcourse.controller

import com.tens.sellcourse.service.DwdSellCourseService
import com.tens.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdSellCourseController {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dwd_sellcourse_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    HiveUtil.useSnappyCompression(sparkSession)

//    DwdSellCourseService.importSaleCourseLog(ssc, sparkSession)
//    DwdSellCourseService.importCoursePay(ssc, sparkSession)
    DwdSellCourseService.importCourseShoppingCart(ssc, sparkSession)

  }
}
