package com.tens.qzpoint.util

import java.io.InputStream
import java.util.Properties

object ConfigurationManager {
  private val is: InputStream = ConfigurationManager.getClass.getClassLoader.getResourceAsStream("comerce.properties")
  private val properties = new Properties()
  properties.load(is)

  def getProperty(propName: String): String = {
    properties.getProperty(propName)
  }
}
