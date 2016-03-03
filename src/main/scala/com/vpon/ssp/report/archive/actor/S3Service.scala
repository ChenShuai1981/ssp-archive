package com.vpon.ssp.report.archive.actor

import java.io.ByteArrayInputStream

import scala.concurrent.{Promise, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain, AWSCredentials}
import com.amazonaws.event.{ProgressEventType, ProgressEvent, ProgressListener}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{PutObjectRequest, ObjectMetadata}
import com.amazonaws.services.s3.transfer.{Upload, TransferManager}
import org.slf4j.LoggerFactory


case class S3Config (
   regionName: String,
   bucketName: String,
   needCompress: Boolean,
   needEncrypt: Boolean
)

class S3Service(s3Config: S3Config) {

  val logger = LoggerFactory.getLogger("S3Service")

  val awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
  val credentials: AWSCredentials = awsCredentialsProvider.getCredentials

  val regionName = s3Config.regionName
  val region = RegionUtils.getRegion(regionName)

  val tx: TransferManager = new TransferManager(credentials)
  val s3: AmazonS3 = tx.getAmazonS3Client
  s3.setRegion(region)

  val s3BucketName: String = s3Config.bucketName

  validateBucket(s3BucketName)

  // TODO
  val needCompress: Boolean = s3Config.needCompress
  val needEncrypt: Boolean = s3Config.needEncrypt

  private def asyncUpload(archiveS3File: ArchiveS3File): Future[Boolean] = {
    val p = Promise[Boolean]()
    val progressListener = new ProgressListener() {
      def progressChanged(progressEvent: ProgressEvent) {
        progressEvent.getEventType match {
          case ProgressEventType.TRANSFER_COMPLETED_EVENT => {
            logger.debug("===> upload completed")
            p.success(true)
          }
          case ProgressEventType.TRANSFER_FAILED_EVENT => {
            logger.debug("===> upload failure")
            p.success(false)
          }
          case et @ _ => {
//            logger.debug("===> met " + et)
          }
        }
      }
    }
    val s3Object: ByteArrayInputStream = new ByteArrayInputStream(archiveS3File.content)
    val metadata = new ObjectMetadata
    val putObjectRequest = new PutObjectRequest(s3BucketName, archiveS3File.key, s3Object, metadata)
    val upload: Upload = tx.upload(putObjectRequest)
    upload.addProgressListener(progressListener)

    p.future
  }

  def send(archiveS3Files: List[ArchiveS3File], partitionId: Option[Int] = None): Future[Int] = {
    val futures = archiveS3Files.map(s3File => {
      asyncUpload(s3File) map (_ match {
        case true => 1
        case false => 0
      })
    })

    Future.sequence(futures) map {a => a.reduceLeft(_ + _)}
  }

  private def validateBucket(bucketName: String) {
    if (!s3.doesBucketExist(bucketName)) {
      throw new ResourceNotFoundException(s"bucket $bucketName does NOT exist")
    }
  }

}