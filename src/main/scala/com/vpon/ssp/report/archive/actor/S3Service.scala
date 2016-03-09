package com.vpon.ssp.report.archive.actor

import java.io.ByteArrayInputStream

import scala.concurrent.{Promise, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain, AWSCredentials}
import com.amazonaws.event.{ProgressEventType, ProgressEvent, ProgressListener}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{PutObjectRequest, ObjectMetadata}
import com.amazonaws.services.s3.transfer.{Upload, TransferManager}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory


case class S3Config (
   regionName: String,
   bucketName: String,
   needCompress: Boolean,
   needEncrypt: Boolean
)

case class S3File(key: String, content: Array[Byte])

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

  class S3UploadListener(p: Promise[Boolean], upload: Upload) extends ProgressListener {
    @Override
    def progressChanged(progressEvent: ProgressEvent) {
      progressEvent.getEventType match {
        case ProgressEventType.TRANSFER_COMPLETED_EVENT => {
          logger.debug("===> upload completed")
          p.success(true)
        }
        case ProgressEventType.TRANSFER_FAILED_EVENT => {
          val cause: AmazonClientException = getException()
          logger.error("===> upload failure: " + ExceptionUtils.getStackTrace(cause))
          p.success(false)
        }
        case et @ _ => {
//          logger.debug("===> met " + et)
        }
      }
    }

    private def getException(): AmazonClientException = {
      try {
        upload.waitForException()
      } catch {
        case e: InterruptedException => {
          Thread.currentThread().interrupt()
          throw new Error("Interrupted when get upload exception.", e)
        }
      }
    }
  }

  private def asyncUpload(s3File: S3File): Future[Boolean] = {
    val p = Promise[Boolean]()
    val buf = s3File.content
    val s3Object: ByteArrayInputStream = new ByteArrayInputStream(buf)
    val s3Key = s3File.key
    logger.debug(s"s3Key ==> $s3Key, bufSize ==> ${buf.size}")
    val metadata = new ObjectMetadata
    val putObjectRequest = new PutObjectRequest(s3BucketName, s3Key, s3Object, metadata)
    // Fix com.amazonaws.ResetException: Content length exceeded the reset buffer limit of 131073;
    // If the request involves an input stream, the maximum stream buffer size can be configured via request.getRequestClientOptions().setReadLimit(int)
    putObjectRequest.getRequestClientOptions().setReadLimit(buf.size)
    val upload: Upload = tx.upload(putObjectRequest)
    val listener = new S3UploadListener(p, upload)
    upload.addProgressListener(listener)
    p.future
  }

  def send(s3Files: Iterable[S3File], partitionId: Option[Int] = None): Future[Int] = {
    val futures = s3Files.map(s3File => {
      asyncUpload(s3File) map (_ match {
        case true => 1
        case false => 0
      })
    })

    Future.sequence(futures) map {a => a.reduceLeft(_ * _)}
  }

  private def validateBucket(bucketName: String) {
    if (!s3.doesBucketExist(bucketName)) {
      throw new ResourceNotFoundException(s"bucket $bucketName does NOT exist")
    }
  }

}
