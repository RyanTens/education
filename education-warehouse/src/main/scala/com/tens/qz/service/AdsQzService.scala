package com.tens.qz.service

import com.tens.qz.dao.AdsQzDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object AdsQzService {
  def getTargetApi(sparkSession: SparkSession, dt: String) = {
    import org.apache.spark.sql.functions._

    val avgTimeAndScore = sparkSession.sql(
      s"""
         |SELECT
         |paperviewid ,
         |paperviewname ,
         |score ,
         |spendtime ,
         |dt ,
         |dn
         |FROM dws.dws_user_paper_detail
         |""".stripMargin)
      .where(s"dt=$dt")
      .groupBy("paperviewid","paperviewname", "dt", "dn")
      .agg(
        avg("score").cast("decimal(4,1)").as("avgscore"),
        avg("spendtime").cast("decimal(10,1)").as("avgspendtime")
      )
      .select("paperviewid", "paperviewname", "avgscore", "avgspendtime", "dt", "dn")
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_paper_avgtimeandscore")

    val maxDetail = sparkSession.sql(
      s"""
         |SELECT
         |paperviewid ,
         |paperviewname ,
         |score ,
         |spendtime ,
         |dt ,
         |dn
         |FROM dws.dws_user_paper_detail
         |""".stripMargin)
      .where(s"dt=$dt")
      .groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(
        max("score").cast("decimal(4, 1)").as("maxscore"),
        min("score").cast("decimal(4, 1)").as("minscore")
      )
      .select("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_paper_maxdetail")

    val top3UserDetail = sparkSession.sql(
      s"""
         |SELECT
         |userid,
         |paperviewid,
         |paperviewname,
         |chaptername,
         |pointname,
         |sitecoursename,
         |coursename,
         |majorname,
         |shortname,
         |papername,
         |score,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |""".stripMargin)
      .where(s"dt=$dt")
      .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
      .where(s"rk < 4")
      .select("userid",
        "paperviewid",
        "paperviewname",
        "chaptername",
        "pointname",
        "sitecoursename",
        "coursename",
        "majorname",
        "shortname",
        "papername",
        "score",
        "rk",
        "dt",
        "dn")
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")

    val low3UserDetail = sparkSession.sql(
      s"""
         |SELECT
         |userid,
         |paperviewid,
         |paperviewname,
         |chaptername,
         |pointname,
         |sitecoursename,
         |coursename,
         |majorname,
         |shortname,
         |papername,
         |score,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |""".stripMargin)
      .where(s"dt=$dt")
      .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy(asc("score"))))
      .where(s"rk < 4")
      .select(
        "userid",
        "paperviewid",
        "paperviewname",
        "chaptername",
        "pointname",
        "sitecoursename",
        "coursename",
        "majorname",
        "shortname",
        "papername",
        "score",
        "rk",
        "dt",
        "dn")
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_low3_userdetail")

    val scoreSegmentUser = sparkSession.sql(
      s"""
         |SELECT
         |paperviewid,
         |paperviewname,
         |userid,
         |score,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |""".stripMargin)
      .where(s"dt=$dt")
      .withColumn(
        "score_segment",
        when(col("score").between(0, 20), "0-20")
          .when(col("score") > 20 && col("score") <= 40, "20-40")
          .when(col("score") > 40 && col("score") <= 60, "40-60")
          .when(col("score") > 60 && col("score") <= 80, "60-80")
          .when(col("score") > 80 && col("score") <= 100, "80-100")
      )
      .drop("score")
      .groupBy("paperviewid", "paperviewname", "score_segment", "dt", "dn")
      .agg(
        concat_ws(",", collect_list("userid").cast("string").as("userids"))
          .as("userids")
      )
      .select("paperviewid",
        "paperviewname",
        "score_segment",
        "userids",
        "dt",
        "dn"
      )
      .orderBy("paperviewid", "score_segment")
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_paper_scoresegment_user")

    sparkSession.sql(
      s"""
         |SELECT
         |paperviewid,
         |paperviewname,
         |score,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |""".stripMargin)
      .where(s"dt=$dt")
      .groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(
        sum(when(col("score") < 60, 1)).as("unpasscount"),
        sum(when(col("score") >= 60, 1)).as("passcount")
      )
      .withColumn(
        "rate",
        col("passcount")./(col("passcount") + col("unpasscount"))
          .cast("decimal(4, 2)")
      )
      .select(
        "paperviewid",
        "paperviewname",
        "unpasscount",
        "passcount",
        "rate",
        "dt",
        "dn"
      )
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")

    val userQuestionDetail = sparkSession.sql(
      s"""
         |SELECT
         |questionid,
         |user_question_answer,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |WHERE dt=$dt
         |""".stripMargin).cache()

    val userQuestionRight = userQuestionDetail
      .where("user_question_answer = '1' ")
      .drop("user_question_answer")
      .groupBy("questionid", "dn", "dt")
      .agg(count("questionid").as("rightcount"))

    val userQuestionError = userQuestionDetail
      .where("user_question_answer = '0' ")
      .drop("user_question_answer")
      .groupBy("questionid", "dt", "dn")
      .agg(count("questionid").as("errcount"))

    val result = userQuestionError
      .join(userQuestionRight, Seq("questionid","dt", "dn"))
      .withColumn(
        "rate",
        (col("errcount") / (col("errcount") + col("rightcount")))
          .cast("decimal(4, 2)")
      )
      .orderBy(desc("errcount"))
      .coalesce(3)
      .select(
        "questionid",
        "errcount",
        "rightcount",
        "rate",
        "dt",
        "dn"
      )
      .coalesce(3)
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("ads.ads_user_question_detail")

  }

  def getTarget(sparkSession: SparkSession, dt: String) = {

      //基于宽表统计各试卷平均耗时、平均分
    AdsQzDao.getAvgSpendTimeAndScore(sparkSession, dt)
      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .insertInto("ads.ads_paper_avgtimeandscore")

    //统计各试卷最高分、最低分
    AdsQzDao.getTopScore(sparkSession, dt)
      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .insertInto("ads.ads_paper_maxdetail")

    AdsQzDao.getTop3UserDetail(sparkSession, dt)
      //按试卷分组统计每份试卷的前三用户详情
      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .insertInto("ads.ads_top3_userdetail")

    //按试卷分组统计每份试卷的倒数前三的用户详情
    AdsQzDao.getLow3UserDetail(sparkSession, dt)
      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .insertInto("ads.ads_low3_userdetail")

    //统计各试卷各分段的用户id，分段有0-20,20-40,40-60,60-80,80-100
    AdsQzDao.getPaperScoreSegmentUser(sparkSession, dt)
      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .insertInto("ads.ads_paper_scoresegment_user")

    //统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60
    AdsQzDao.getPaperPassDetail(sparkSession, dt)
      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .insertInto("ads.ads_user_paper_detail")

    //统计各题的错误数，正确数，错题率
    AdsQzDao.getQuestionDetail(sparkSession, dt)
      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .insertInto("ads.ads_user_question_detail")

  }

}
