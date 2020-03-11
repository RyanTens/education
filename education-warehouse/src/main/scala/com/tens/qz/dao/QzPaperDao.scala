package com.tens.qz.dao

import org.apache.spark.sql.SparkSession

object QzPaperDao {

  def getDwdQzPaperView(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |paperviewid,
         |paperid,
         |paperviewname,
         |paperparam,
         |openstatus,
         |explainurl,
         |iscontest,
         |contesttime,
         |conteststarttime,
         |contestendtime,
         |contesttimelimit,
         |dayiid,
         |status,
         |creator as paper_view_creator,
         |createtime as paper_view_createtime,
         |paperviewcatid,
         |modifystatus,
         |description,
         |papertype,
         |downurl,
         |paperuse,
         |paperdifficult,
         |testreport,
         |paperuseshow,
         |dt,
         |dn
         |from dwd.dwd_qz_paper_view
         |where dt='$dt'
         |""".stripMargin)
  }

  def getDwdQzCenterPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |paperviewid,
         |sequence,
         |centerid,
         |dn
         |from dwd.dwd_qz_center_paper
         |where dt='$dt'
         |""".stripMargin)
  }

  def getDwdQzPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |paperid,
         |papercatid,
         |courseid,
         |paperyear,
         |chapter,
         |suitnum,
         |papername,
         |totalscore,
         |chapterid,
         |chapterlistid,
         |dn
         |from dwd.dwd_qz_paper
         |where dt='$dt'
         |""".stripMargin)
  }

  def getDwdQzCenter(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |centerid,
         |centername,
         |centeryear,
         |centertype,
         |centerparam,
         |provideuser,
         |centerviewtype,
         |stage,
         |dn
         |from dwd.dwd_qz_center
         |where dt='$dt'
         |""".stripMargin)
  }
}
