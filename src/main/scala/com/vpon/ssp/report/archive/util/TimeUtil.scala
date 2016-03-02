package com.vpon.ssp.report.archive.util

import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter


object TimeUtil {

  val redshiftDateTimePattern = "yyyy-MM-dd hh:mm:ss"
  val fmt: DateTimeFormatter = DateTimeFormat.forPattern(redshiftDateTimePattern).withZoneUTC()

  def convertTimestampToString(timestamp: Long): String = {
    fmt.print(timestamp)
  }
}
