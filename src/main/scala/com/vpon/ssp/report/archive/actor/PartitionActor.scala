package com.vpon.ssp.report.archive.actor

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import akka.actor.SupervisorStrategy
import akka.actor._
import akka.util.Timeout

import com.amazonaws.services.s3.model.AmazonS3Exception
import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.document.StringDocument
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{StringDecoder, DefaultDecoder}
import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import com.vpon.ssp.report.archive.actor.PartitionActorProtocol._
import com.vpon.ssp.report.archive.actor.PartitionMetricsProtocol._
import com.vpon.ssp.report.archive.config.ArchiveConfig
import com.vpon.ssp.report.archive.couchbase.CBExtension
import com.vpon.ssp.report.archive.kafka.consumer.TopicsConsumer
import com.vpon.ssp.report.archive.kafka.consumer.TopicsConsumer.AbsoluteOffset
import com.vpon.ssp.report.archive.util.{S3Util, TimeUtil, Retry}
import com.vpon.ssp.report.archive.util.Retry.NeedRetryException

case class MessageFile(offset: Long, dateString: String, kv: (String, Array[Byte]))

object PartitionActorProtocol {
  case class JobRequest(mmds: List[MessageAndMetadata[String, Array[Byte]]])
  case object ResumeWork
  case object ResetWork
  case object PauseWork
  case object NextJob
  case class Fulfill(currentSourceEpic: Option[CustomizedIterator[Array[Byte], DefaultDecoder]])
}

object PartitionActor {
  def props(partitionId: Int, master: ActorRef): Props = Props(new PartitionActor(partitionId, master))
}

class PartitionActor(val partitionId: Int, val master:ActorRef) extends Actor with ActorLogging with ArchiveConfig {
  import context._

  watch(master)

  private val maxRetries = 10
  private val timeRange = 1.minute

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = maxRetries, withinTimeRange = timeRange) {
      case _: ArithmeticException      => SupervisorStrategy.Resume
      case _: NullPointerException     => SupervisorStrategy.Restart
      case _: IllegalArgumentException => SupervisorStrategy.Stop
      case _: Exception                => SupervisorStrategy.Escalate
    }

  implicit val timeout = Timeout(10.second)
  private var isWorkable = true

  private val partitionMetrics = context.actorOf(PartitionMetrics.props(partitionId), s"archive-metrics-${partitionId}")

  private var currentSourceEpic: Option[CustomizedIterator[Array[Byte], DefaultDecoder]] = None

  private val sourceKafkaParams = Map("metadata.broker.list" -> sourceBrokers)
  private val sourceTopicPartition = TopicAndPartition(sourceTopic, partitionId)

  private val offsetBucketWithKeyPrefix = CBExtension(system).buckets("offset")
  private val offsetBucket          = offsetBucketWithKeyPrefix.bucket
  private val offsetBucketKeyPrefix = offsetBucketWithKeyPrefix.keyPrefix

  private val cbOffsetKey          = s"${offsetBucketKeyPrefix}${partitionId}"

  @volatile
  private var sourceTopicsConsumer: Future[TopicsConsumer[String, Array[Byte], StringDecoder, DefaultDecoder, MessageAndMetadata[String, Array[Byte]]]] = _

  private val s3Config = new S3Config(
    regionName = s3RegionName,
    bucketName = s3BucketName,
    needEncrypt = s3NeedEncrypt
  )

  private val s3Service = new S3Service(s3Config)

  override def preStart: Unit = {
    log.debug(s"${self.path} ==> PartitionActor preStart")
    super.preStart
    initOffset
  }

  def receive: akka.actor.Actor.Receive = {
    case jr@JobRequest(mmds) => handleJobRequest(mmds)

    case NextJob => fetchNextJob

    case Fulfill(epic) => {
      currentSourceEpic = epic
      self ! NextJob
    }

    case PauseWork => {
      isWorkable = false
      partitionMetrics ! IsWorking(isWorkable)
    }
    case ResumeWork => {
      isWorkable = true
      initOffset
      partitionMetrics ! Resume
    }
    case ResetWork => {
      isWorkable = true
      initOffset
      partitionMetrics ! Reset
    }
    case Terminated(partitionMaster) =>
      log.warning(s"${self.path} ==> PartitionMaster $partitionMaster got terminated!!")
  }

  private def handleJobRequest(mmds: List[MessageAndMetadata[String, Array[Byte]]]) = {
    log.debug(s"${self.path} ==> [STEP 1.1] start handleJobRequest")
    mmds.isEmpty match {
      case true => {
        log.debug(s"${self.path} ==> [STEP 1.2] received 0 batch message")
        scheduleNextJob
      }
      case false => {
        log.debug(s"${self.path} ==> [STEP 1.2] received ${mmds.size} batch messages")
        doWork(mmds)
      }
    }
  }

  private def scheduleNextJob() = system.scheduler.scheduleOnce(1.seconds, self, NextJob)

  private def fetchNextJob() = {
    log.debug(s"${self.path} ==> fetchNextJob")
    isWorkable match {
      case true => {
        currentSourceEpic match {
          case None => {
            log.debug(s"${self.path} ==> No more job need to be processed due to no more unprocessed messages in partition-${partitionId} of source topic")
            scheduleNextJob
          }
          case Some(epic) => {
            val iterator = currentSourceEpic.get
            val mmds = iterator.next.toList
            val currentBatchSize = mmds.size
            if (currentBatchSize > 0) log.info(s"${self.path} ==> current batch size: ${currentBatchSize}")
            self ! JobRequest(mmds)
          }
        }
      }
      case false => {
        log.debug(s"${self.path} ==> No more job need to be processed due to PartitionActor-${partitionId} was paused")
        scheduleNextJob
      }
    }
  }

  def initOffset = {
    val offsetF: Future[Long] = retryCouchbaseOperation{
      log.debug(s"${self.path} ==> cbOffsetKey: $cbOffsetKey")
      offsetBucket.get[StringDocument](cbOffsetKey)
    }.map(_.content) map { f =>
      log.debug(s"${self.path} ==> offset: ${f.toLong}")
      f.toLong
    } recover {
      case e @ (_: com.couchbase.client.java.error.DocumentDoesNotExistException | _: java.lang.NullPointerException) => {
        0
      }
    }

    sourceTopicsConsumer = offsetF map { offset =>
      log.debug(s"${self.path} ==> LastOffset: $offset")
      partitionMetrics ! LastOffset(offset)
      log.debug(s"${self.path} ==> constructing TopicsConsumer: sourceKafkaParams -> $sourceKafkaParams, sourceTopicPartition -> $sourceTopicPartition, AbsoluteOffset -> $offset")
      TopicsConsumer[String, Array[Byte], StringDecoder, DefaultDecoder](sourceKafkaParams, Set(sourceTopicPartition), AbsoluteOffset(offset))
    }

    sourceTopicsConsumer onComplete {
      case Success(s) => {
        log.debug(s"${self.path} ==> sourceTopicsConsumer success. consumeBatchSize: $consumeBatchSize")
        self ! Fulfill(Some(new CustomizedIterator[Array[Byte], DefaultDecoder](consumeBatchSize, s)))
      }
      case Failure(e) =>
        val err = s"${self.path} ==> sourceTopicsConsumer failure which results failed to start PartitionActor for partition $partitionId, so pause work!!\n${ExceptionUtils.getStackTrace(e)}"
        log.error(e, err)
        partitionMetrics ! Error(err)
        self ! PauseWork
    }
  }

  def doWork(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[Any] = {
    val f = for {
      sendResult <- send(mmds)
    } yield {
        sendResult match {
          case None => {
            log.debug(s"${self.path} ==> [STEP 2.3] empty data set. But continue to process next batch messages without blocking")
            doPostSendActions(mmds)
          }
          case Some(isSentSuccess) => {
            if (isSentSuccess) {
              log.debug(s"${self.path} ==> [STEP 2.3] sent success. continue to doPostSendActions.")
              doPostSendActions(mmds)
            } else {
              val err = s"${self.path} ==> [STEP 2.3] sent failure"
              log.error(err)
              partitionMetrics ! Error(err)
              self ! PauseWork
            }
          }
        }
      }

    f.recover{
      case e @ (_: AmazonS3Exception | _: CouchbaseException) => {
        val time = System.currentTimeMillis()
        log.error(e, e.getMessage)
        partitionMetrics ! Error(e.getMessage)
        log.error(s"${self.path} ==> [STEP 3.5] Caught fatal exception ${e.getClass} when doWork at $time, so pause work!", e)
        self ! PauseWork
      }
    }
  }

  private def convertMmdToMessageFile(mmd: MessageAndMetadata[String, Array[Byte]]): MessageFile = {
      val eventKey = mmd.key()
      val eventOffset = mmd.offset
      val eventTimestamp = eventKey.split("_").head.toLong
      val dateString = TimeUtil.convertToS3TimestampString(eventTimestamp)
      //log.debug(s"${self.path} ==> eventTimestamp = $eventTimestamp, dateString = $dateString")
      val kv = (eventKey, mmd.message())
      MessageFile(eventOffset, dateString, kv)
  }

  private def send(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[Option[Boolean]] = {
    log.debug(s"${self.path} ==> [STEP 2.1] start send")
    if (!mmds.isEmpty) {
      val allMessageFiles = mmds.map(convertMmdToMessageFile(_)) // default sort by offset
      val sendStartTime = System.currentTimeMillis()
      val s3Files = allMessageFiles.groupBy(_.dateString).map(kv => {
        val dateString = kv._1 // yyyy/MM/dd/HH/mm
        val batchMessageFiles = kv._2.sortBy(_.offset)
        val batchArrayBytes: Array[(String, Array[Byte])] = batchMessageFiles.map(file => file.kv).toArray
        val beforeCompressionData = SerializationUtils.serialize(batchArrayBytes)
        val (s3Content, fileSuffix) = compressOrNot(beforeCompressionData, s3CompressionType)

        // topicName.yyyy.MM.dd.HH.mm.partitionId.(lastOffset+1).size
        val s3FileName = S3Util.getS3FileName(sourceTopic, dateString, partitionId, batchMessageFiles.last.offset, batchMessageFiles.size, fileSuffix)
        val s3Folder = S3Util.getS3Folder(sourceTopic, dateString, partitionId)
        val s3Key = S3Util.getS3Key(s3Folder, s3FileName)
        S3File(s3Key, s3Content, s3FileName)
      })

      s3Service.send(s3Files, Some(partitionId)).map(k => {
        val sendTime = System.currentTimeMillis() - sendStartTime
        partitionMetrics ! Send(sendTime)
        log.debug(s"${self.path} ==> [STEP 2.2] end send with true result.")
        k == 1 match {
          case true => Some(true)
          case false => Some(false)
        }
      })
    } else {
      log.debug(s"${self.path} ==> [STEP 2.2] end send with None result.")
      Future { None }
    }
  }

  private def compressOrNot(inputData: Array[Byte], compressionType: String): (Array[Byte], String) = {
    log.debug(s"${self.path} ==> [STEP 5.3] compressOrNot. compressionType: $compressionType ")
    compressionType match {
      case "GZIP" => {
        (GZIPCompressor.compressData(inputData), "gz")
      }
      case "BZIP2" => {
        (BZIP2Compressor.compressData(inputData), "bz2")
      }
      case "LZOP" => {
        (LZOPCompressor.compressData(inputData), "lzo")
      }
      case _ => (inputData, "csv")
    }
  }

  private def doPostSendActions(mmds: List[MessageAndMetadata[String, Array[Byte]]]) = {
    val f = for {
      updateLastOffsetResult <- updateLastOffset(mmds)
    } yield {
        self ! NextJob
      }

    f.recover{
      case e:CouchbaseException => {
        val time = System.currentTimeMillis()
        log.error(e, e.getMessage)
        partitionMetrics ! Error(e.getMessage)
        log.error(s"${self.path} ==> Caught CouchbaseException when doPostSendActions at $time, so pause work!!.", e)
        self ! PauseWork
      }
      case t:Throwable => {
        val time = System.currentTimeMillis()
        log.warning(s"${self.path} ==> Caught Throwable when doPostSendActions at $time, but still continue fetch next job.", t)
        self ! NextJob
      }
    }
  }

  private def updateLastOffset(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[Option[Boolean]] = {
    log.debug(s"${self.path} ==> [STEP 3.1] start updateLastOffset")
    if (!mmds.isEmpty) {
      val lastOffset = mmds.last.offset
      val toSaveOffset = lastOffset + 1
      log.debug(s"${self.path} ==> [STEP 3.2] toSaveOffset: $toSaveOffset")
      retryCouchbaseOperation{
        offsetBucket.upsert(StringDocument.create(cbOffsetKey, toSaveOffset.toString))
      } map {
        strDoc => {
          log.info(s"${self.path} ==> [STEP 3.3] Success to update last offset: $cbOffsetKey -> $toSaveOffset.")
          partitionMetrics ! LastOffset(toSaveOffset)
          log.debug(s"${self.path} ==> [STEP 3.4] end updateLastOffset with true result.")
          Some(true)
        }
      } recover {
        case e: Throwable => {
          val err = s"${self.path} ==> [STEP 3.3] Failed to update last offset: $cbOffsetKey -> $toSaveOffset\n${ExceptionUtils.getStackTrace(e)}"
          throw new CouchbaseException(err)
        }
      }
    } else {
      log.debug(s"${self.path} ==> [STEP 3.2] end updateLastOffset with None result.")
      Future { None }
    }
  }

  def retryCouchbaseOperation[T](task: Future[T]): Future[T] = {
    val retryTask = Retry(system.scheduler, couchbaseMaxRetries, couchbaseRetryInterval) {
      task.recover {
        case e: com.couchbase.client.core.RequestCancelledException =>
          val msg = s"com.couchbase.client.core.RequestCancelledException ==> Try again after ${couchbaseRetryInterval}ms. ${ExceptionUtils.getStackTrace(e)}"
          log.warning(msg)
          throw new NeedRetryException(msg)
        case e: com.couchbase.client.java.error.TemporaryFailureException =>
          val msg = s"com.couchbase.client.java.error.TemporaryFailureException ==> Try again after ${couchbaseRetryInterval}ms. ${ExceptionUtils.getStackTrace(e)}"
          log.warning(msg)
          throw new NeedRetryException(msg)
        case e: java.net.ConnectException =>
          val msg = s"java.net.ConnectException ==> Try again after ${couchbaseRetryInterval}ms. ${ExceptionUtils.getStackTrace(e)}"
          log.warning(msg)
          throw new NeedRetryException(msg)
        case e: RuntimeException if (e.getMessage.equals("java.util.concurrent.TimeoutException")) =>
          val msg = s"RuntimeException (java.util.concurrent.TimeoutException) ==> Try again after ${couchbaseRetryInterval}ms. ${ExceptionUtils.getStackTrace(e)}"
          log.warning(msg)
          throw new NeedRetryException(msg)
      }
    }

    retryTask.recover {
      case e: NeedRetryException => {
        log.error(s"Failed to retryCouchbaseAction.")
        throw new CouchbaseException(e.getMessage)
      }
    }
  }
}
