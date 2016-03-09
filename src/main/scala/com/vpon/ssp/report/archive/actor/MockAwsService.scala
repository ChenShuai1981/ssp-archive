package com.vpon.ssp.report.archive.actor

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


class MockAwsService(awsConfig: S3Config) {

  def send(archiveS3Files: List[S3File], partitionId: Option[Int] = None): Future[Int] = {
    val futures = archiveS3Files.map(s3File => {
      asyncUpload(s3File) map (_ match {
        case true => 1
        case false => 0
      })
    })

    Future.sequence(futures) map {a => a.reduceLeft(_ + _)}
  }

  private def asyncUpload(archiveS3File: S3File): Future[Boolean] = {
    println(s"uploading s3 file: ${archiveS3File.key} => ${archiveS3File.content}")
    Future(true)
  }
}
