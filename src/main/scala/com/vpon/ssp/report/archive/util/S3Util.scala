package com.vpon.ssp.report.archive.util

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object S3Util {

  val s3DateTimePattern = "yyyy/MM/dd/HH/mm/"

  val fmt: DateTimeFormatter = DateTimeFormat.forPattern(s3DateTimePattern).withZoneUTC()

  def getS3Folder(eventTime: Long, topic: String = ""): String = {
    val dateStr = fmt.print(eventTime)
    topic + "/" + dateStr
  }

  def getS3FileName(messageKey: String): String = s"${messageKey}"

}
