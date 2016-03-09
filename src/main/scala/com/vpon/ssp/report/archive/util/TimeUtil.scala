package com.vpon.ssp.report.archive.util

import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter


object TimeUtil {

  val s3DateTimePattern = "yyyy/MM/dd/HH/mm"

  val s3Fmt: DateTimeFormatter = DateTimeFormat.forPattern(s3DateTimePattern).withZoneUTC()

  def convertToS3TimestampString(eventTime: Long) = s3Fmt.print(eventTime)
}
