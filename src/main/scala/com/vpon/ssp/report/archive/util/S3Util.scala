package com.vpon.ssp.report.archive.util


object S3Util {

  val s3Delimiter = "/"
  val seperator = "."
  val period = "minutely"
  val fileSuffix = ".arc"

  // ssp-edge-events.2016.03.09.05.39.0.100.100.arc
  def getS3FileName(sourceTopic: String, dateString: String /* yyyy/MM/dd/HH/mm */, partitionId: Int, lastOffset: Long, batchSize: Int): String =
    sourceTopic + seperator + dateString.replaceAll(s3Delimiter, seperator) + seperator + partitionId + seperator + (lastOffset+1) + seperator + batchSize + seperator + fileSuffix

  def getS3Folder(sourceTopic: String, dateString: String /* yyyy/MM/dd/HH/mm */, partitionId: Int): String =
    "topics" + s3Delimiter + sourceTopic + s3Delimiter + period + s3Delimiter + dateString + s3Delimiter + partitionId

  def getS3Key(s3Folder: String, s3FileName: String): String = s3Folder + s3Delimiter + s3FileName

}
