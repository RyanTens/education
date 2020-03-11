package com.tens.sellcourse.bean

case class DwdCourseShoppingCart (courseid: Int,
                                  orderid:String,
                                  coursename:String,
                                  cart_discount: java.math.BigDecimal,
                                  sellmoney: java.math.BigDecimal,
                                  cart_createtime: java.sql.Timestamp,
                                  dt: String,
                                  dn: String,
                                  rand_courseid: String)
