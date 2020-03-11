package com.tens.sellcourse.service

import com.tens.sellcourse.bean.{DwdCourseShoppingCart, DwdSaleCourse}
import com.tens.sellcourse.dao.DwdSellCourseDao
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DwsSellCourseService {
  /**
   * 数据倾斜解决方案3：采用广播小表 两个大表进行分桶并且进行SMB join,
   * join之后的大表再与广播后的小表进行join
   * @param sparkSession
   * @param dt
   */
  def importSellCourseDetail4(sparkSession: SparkSession, dt: String) = {
    val dwdSaleCourse = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt='$dt' ")
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart2(sparkSession).where(s"dt='${dt}'")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay2(sparkSession).where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    import org.apache.spark.sql.functions._
    val tmpdata = dwdCourseShoppingCart.join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
    val result = broadcast(dwdSaleCourse).join(tmpdata, Seq("courseid", "dt", "dn"), "right")
    result.select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
      , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
      "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")

  }


  /**
    * 数据倾斜解决方案2：采用广播小表
   *
   * @param sparkSession
    * @param dt
    */
  def importSellCourseDetail3(sparkSession: SparkSession, dt: String) = {
    val dwdSaleCourse =
      DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
    val dwdCourseShoppingCart = DwdSellCourseDao
      .getDwdCourseShoppingCart(sparkSession)
      .where(s"dt='${dt}'")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay = DwdSellCourseDao
      .getDwdCoursePay(sparkSession)
      .where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    import org.apache.spark.sql.functions._
    broadcast(dwdSaleCourse)
      .join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select(
        "courseid",
        "coursename",
        "status",
        "pointlistid",
        "majorid",
        "chapterid",
        "chaptername",
        "edusubjectid",
        "edusubjectname",
        "teacherid",
        "teachername",
        "coursemanager",
        "money",
        "orderid",
        "cart_discount",
        "sellmoney",
        "cart_createtime",
        "pay_discount",
        "paymoney",
        "pay_createtime",
        "dt",
        "dn"
      )
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")

  }

  /**
    * 数据倾斜 解决方案1：将小表进行扩容 大表key加上随机散列值
    *
    * @param sparkSession
    * @param dt
    */
  def importSellCourseDetail2(sparkSession: SparkSession, dt: String) = {
    val dwdSaleCourse =
      DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
    val dwdCourseShoppingCart = DwdSellCourseDao
      .getDwdCourseShoppingCart(sparkSession)
      .where(s"dt='${dt}'")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val dwdCoursePay = DwdSellCourseDao
      .getDwdCoursePay(sparkSession)
      .where(s"dt='${dt}'")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    import sparkSession.implicits._
    //大表的key 拼随机前缀
    val newdwdCourseShoppingCart = dwdCourseShoppingCart
      .mapPartitions(partitions => {
        partitions.map(item => {
          val courseid = item.getAs[Int]("courseid")
          val randInt = Random.nextInt(100)
          DwdCourseShoppingCart(
            courseid,
            item.getAs[String]("orderid"),
            item.getAs[String]("coursename"),
            item.getAs[java.math.BigDecimal]("cart_discount"),
            item.getAs[java.math.BigDecimal]("sellmoney"),
            item.getAs[java.sql.Timestamp]("cart_createtime"),
            item.getAs[String]("dt"),
            item.getAs[String]("dn"),
            courseid + "_" + randInt
          )
        })
      })
    //小表进行扩容
    val newdwdSaleCourse = dwdSaleCourse.flatMap(item => {
      val list = new ArrayBuffer[DwdSaleCourse]()
      val courseid = item.getAs[Int]("courseid")
      val coursename = item.getAs[String]("coursename")
      val status = item.getAs[String]("status")
      val pointlistid = item.getAs[Int]("pointlistid")
      val majorid = item.getAs[Int]("majorid")
      val chapterid = item.getAs[Int]("chapterid")
      val chaptername = item.getAs[String]("chaptername")
      val edusubjectid = item.getAs[Int]("edusubjectid")
      val edusubjectname = item.getAs[String]("edusubjectname")
      val teacherid = item.getAs[Int]("teacherid")
      val teachername = item.getAs[String]("teachername")
      val coursemanager = item.getAs[String]("coursemanager")
      val money = item.getAs[java.math.BigDecimal]("money")
      val dt = item.getAs[String]("dt")
      val dn = item.getAs[String]("dn")
      for (i <- 0 until 100) {
        list.append(
          DwdSaleCourse(
            courseid,
            coursename,
            status,
            pointlistid,
            majorid,
            chapterid,
            chaptername,
            edusubjectid,
            edusubjectname,
            teacherid,
            teachername,
            coursemanager,
            money,
            dt,
            dn,
            courseid + "_" + i
          )
        )
      }
      list.toIterator
    })
    newdwdSaleCourse
      .join(
        newdwdCourseShoppingCart.drop("courseid").drop("coursename"),
        Seq("rand_courseid", "dt", "dn"),
        "right"
      )
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select(
        "courseid",
        "coursename",
        "status",
        "pointlistid",
        "majorid",
        "chapterid",
        "chaptername",
        "edusubjectid",
        "edusubjectname",
        "teacherid",
        "teachername",
        "coursemanager",
        "money",
        "orderid",
        "cart_discount",
        "sellmoney",
        "cart_createtime",
        "pay_discount",
        "paymoney",
        "pay_createtime",
        "dt",
        "dn"
      )
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("dws.dws_salecourse_detail")

  }

  /**
    * 小表join大表 未优化 数据倾斜
    * @param sparkSession
    * @param dt
    */
  def importSellCourseDetail(sparkSession: SparkSession, dt: String) = {
    val dwdSaleCourse =
      DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt = '$dt' ")

    val dwdCourseShoppingCart = DwdSellCourseDao
      .getDwdCourseShoppingCart(sparkSession)
      .where(s"dt = '$dt' ")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    val dwdCoursePay = DwdSellCourseDao
      .getDwdCoursePay(sparkSession)
      .where(s"dt = '$dt' ")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")

    dwdSaleCourse
      .join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select(
        "courseid",
        "coursename",
        "status",
        "pointlistid",
        "majorid",
        "chapterid",
        "chaptername",
        "edusubjectid",
        "edusubjectname",
        "teacherid",
        "teachername",
        "coursemanager",
        "money",
        "orderid",
        "cart_discount",
        "sellmoney",
        "cart_createtime",
        "pay_discount",
        "paymoney",
        "pay_createtime",
        "dt",
        "dn"
      )
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("dws.dws_salecourse_detail")

//    DwdSellCourseDao.getDwdCoursePay(sparkSession)

  }

}
