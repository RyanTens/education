package com.tens.qz.dao

import org.apache.spark.sql.SparkSession

object AdsQzDao {
  /**
   * 统计各题的错误数，正确数，错题率
   * @param sparkSession
   * @param dt
   * @return
   */
  def getQuestionDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |t3.questionid,
         |t3.errocount,
         |t3.rightcount,
         |cast(t3.errocount / (t3.errocount + t3.rightcount) as decimal(4,2)) as rate,
         |t3.dt,
         |t3.dn
         |from
         |(select
         |t1.questionid,
         |t1.errocount,
         |t2.rightcount,
         |t1.dt,
         |t1.dn
         |from
         |(SELECT
         |questionid,
         |count(*) AS errocount,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |WHERE dt = '$dt' AND user_question_answer = 0
         |GROUP BY questionid,dt, dn) t1
         |join
         |(SELECT
         |questionid,
         |count(*) AS rightcount,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |WHERE dt = '$dt' AND user_question_answer = 1
         |GROUP BY questionid, dt,dn) t2
         |on t1.questionid = t2.questionid and t1.dn = t2.dn) t3
         |""".stripMargin)
  }

  /**
   * 统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60
   * 已优化
   * @param sparkSession
   * @param dt
   * @return
   */
  def getPaperPassDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |SELECT
         |t1.paperviewid,
         |t1.paperviewname,
         |t1.unpasscount,
         |t1.passcount,
         |cast(t1.passcount / (t1.passcount + t1.unpasscount) AS DECIMAL(4,2)) AS rate,
         |t1.dt,
         |t1.dn
         |from
         |(SELECT
         |paperviewid,
         |paperviewname,
         |sum(if(score < 60, 1, 0)) AS unpasscount,
         |sum(if(score >= 60, 1, 0)) AS passcount,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |WHERE dt = '$dt'
         |GROUP BY paperviewid, paperviewname, dt, dn) t1
         |""".stripMargin)
  }

  /**
   * 统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60
   * 有BUG 未优化
   * @param sparkSession
   * @param dt
   * @return
   */
  def getPaperPassDetail2(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |SELECT
         |t3.paperviewid,
         |t3.paperviewname,
         |t3.unpasscount,
         |t3.passcount,
         |CAST(t3.passcount / (t3.passcount + unpasscount) as decimal(4,2)) as rate,
         |t3.dt,
         |t3.dn
         |FROM
         |(select
         |t1.paperviewid,
         |t1.paperviewname,
         |t1.unpasscount,
         |t1.dt,
         |t1.dn,
         |t2.passcount
         |FROM
         |(SELECT
         |paperviewid,
         |paperviewname,
         |count(*) unpasscount,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |where dt = '$dt' and score between 0 and 60
         |GROUP BY paperviewid, paperviewname, dt, dn) t1
         |join
         |(SELECT
         |paperviewid,
         |count(*) passcount,
         |dn
         |FROM dws.dws_user_paper_detail
         |WHERE dt = '$dt' AND score > 60
         |GROUP BY paperviewid, dn) t2
         |on t1.paperviewid = t2.paperviewid and t1.dn = t2.dn) t3
         |""".stripMargin)
  }

  /**
   * 统计各试卷各分段的用户id，分段有0-20,20-40,40-60,60-80,80-100
   * @param sparkSession
   * @param dt
   * @return
   */
  def getPaperScoreSegmentUser(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |SELECT
         |paperviewid,
         |paperviewname,
         |score_segment,
         |concat_ws(',', collect_list(CAST(userid as string))),
         |dt,
         |dn
         |FROM
         |(SELECT
         |paperviewid,
         |paperviewname,
         |userid,
         |CASE WHEN score >= 0 AND score <= 20 THEN '0-20'
         |WHEN score > 20 AND score <= 40 THEN '20-40'
         |WHEN score > 40 AND score <= 60 THEN '40-60'
         |WHEN score > 60 AND score <= 80 THEN '60-80'
         |WHEN score > 80 AND score <= 100 THEN '80-100'
         |END AS score_segment,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |WHERE dt = '$dt') t1
         |GROUP BY t1.paperviewid, t1.paperviewname, t1.score_segment, dt, dn
         |ORDER BY t1.paperviewid, t1.score_segment
         |""".stripMargin)
  }

  /**
   * 按试卷分组统计每份试卷的倒数前三的用户详情
   * @param sparkSession
   * @param dt
   * @return
   */
  def getLow3UserDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |SELECT
         |*
         |from
         |(SELECT
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
         |rank() OVER(PARTITION BY paperviewid ORDER BY score) as rk,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |WHERE dt = '$dt') t1
         |WHERE rk < 4
         |""".stripMargin)
  }

  /**
   * 按试卷分组统计每份试卷的前三用户详情
   * @param sparkSession
   * @param dt
   * @return
   */
  def getTop3UserDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |SELECT
         |*
         |from
         |(SELECT
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
         |rank() OVER (PARTITION BY paperviewid ORDER BY score DESC) as rk,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |where dt='$dt') t1
         |WHERE t1.rk < 4
         |""".stripMargin)
  }

  /**
   * 统计各试卷最高分、最低分
   * @param sparkSession
   * @param dt
   * @return
   */
  def getTopScore(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |SELECT
         |paperviewid,
         |paperviewname,
         |cast(max(score) as DECIMAL(4, 1)) maxscore,
         |cast(min(score) as DECIMAL(4, 1)) minscore,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |where dt='$dt'
         |GROUP BY paperviewid, paperviewname, dt, dn
         |""".stripMargin)
  }

  /**
   * 基于宽表统计各试卷平均耗时、平均分
   * @param sparkSession
   * @param dt
   * @return
   */
  def getAvgSpendTimeAndScore(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |SELECT
         |paperviewid,
         |paperviewname,
         |cast(avg(score) AS DECIMAL(4,1)) avgscore,
         |cast(avg(spendtime) as DECIMAL(10,1)) avgspendtime,
         |dt,
         |dn
         |FROM dws.dws_user_paper_detail
         |where dt='$dt'
         |GROUP BY paperviewid, paperviewname, dt, dn
         |ORDER BY avgscore DESC, avgspendtime DESC
         |""".stripMargin)
  }

}
