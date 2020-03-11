package com.tens.qz.dao

import org.apache.spark.sql.SparkSession

object QzCourseDao {
  def getDwdQzSiteCourse(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |sitecourseid,
         |siteid,
         |courseid,
         |sitecoursename,
         |coursechapter,
         |sequence,status,
         |creator as sitecourse_creator,
         |createtime as sitecourse_createtime,
         |helppaperstatus,
         |servertype,
         |boardid,
         |showstatus,
         |dt,
         |dn
         |from dwd.dwd_qz_site_course
         |where dt='$dt'
         |""".stripMargin)
  }

  def getDwdQzCourse(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |courseid,
         |majorid,
         |coursename,
         |isadvc,
         |chapterlistid,
         |pointlistid,
         |dn
         |from dwd.dwd_qz_course
         |where dt='$dt'
         |""".stripMargin)
  }

  def getDwdQzCourseEduSubject(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |courseeduid,
         |edusubjectid,
         |courseid,
         |dn
         |from dwd.dwd_qz_course_edusubject
         |where dt='$dt'
         |""".stripMargin)
  }
}
