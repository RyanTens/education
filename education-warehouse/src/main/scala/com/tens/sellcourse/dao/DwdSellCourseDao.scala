package com.tens.sellcourse.dao

import org.apache.spark.sql.SparkSession

object DwdSellCourseDao {
  def getDwdSaleCourse(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |select
         |    courseid,
         |    coursename,
         |    status,
         |    pointlistid,
         |    majorid,
         |    chapterid,
         |    chaptername,
         |    edusubjectid,
         |    edusubjectname,
         |    teacherid,
         |    teachername,
         |    coursemanager,
         |    money,
         |    dt,
         |    dn
         |from
         |    dwd.dwd_sale_course
         |""".stripMargin)
  }

  def getDwdCourseShoppingCart(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |select
         |    courseid,
         |    orderid,
         |    coursename,
         |    discount,
         |    sellmoney,
         |    createtime,
         |    dt,
         |    dn
         |from
         |    dwd.dwd_course_shopping_cart
         |""".stripMargin)
  }

  def getDwdCoursePay(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |select
         |    orderid,
         |    discount,
         |    paymoney,
         |    createtime,
         |    dt,
         |    dn
         |from
         |    dwd.dwd_course_pay
         |""".stripMargin)
  }

  def getDwdCourseShoppingCart2(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |select
         |    courseid,
         |    orderid,
         |    coursename,
         |    discount,
         |    sellmoney,
         |    createtime,
         |    dt,
         |    dn
         |from
         |    dwd.dwd_course_shopping_cart_cluster
         |""".stripMargin)
  }

  def getDwdCoursePay2(sparkSession: SparkSession) = {
    sparkSession.sql(
      s"""
         |select
         |    orderid,
         |    discount,
         |    paymoney,
         |    createtime,
         |    dt,
         |    dn
         |from
         |    dwd.dwd_course_pay_cluster
         |""".stripMargin)
  }

}
