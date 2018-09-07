package com.watson.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * DateUtils
  *
  * Created by watson on 2018/9/5.
  */
object DateUtils {

  var YYYYMMDD = new SimpleDateFormat("yyyy-MM-dd")

  val FULL_FORMAT = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS+0800")

  def getCurrentTime2String(format: SimpleDateFormat, time: Long): String = format.format(time)

  def getISO8601Timestamp(date: Date): String = FULL_FORMAT.format(date)
}
