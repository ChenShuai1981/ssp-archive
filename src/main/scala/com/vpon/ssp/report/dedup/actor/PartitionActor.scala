package com.vpon.ssp.report.dedup.actor

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import akka.actor.SupervisorStrategy
import akka.actor._
import akka.util.Timeout

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.document.StringDocument
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{StringDecoder, DefaultDecoder}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.common.KafkaException
import com.vpon.ssp.report.dedup.actor.PartitionActorProtocol._
import com.vpon.ssp.report.dedup.actor.PartitionMetricsProtocol._
import com.vpon.ssp.report.dedup.config.DedupConfig
import com.vpon.ssp.report.dedup.couchbase.CBExtension
import com.vpon.ssp.report.dedup.kafka.consumer.TopicsConsumer
import com.vpon.ssp.report.dedup.kafka.consumer.TopicsConsumer.AbsoluteOffset
import com.vpon.ssp.report.dedup.util.{S3Util, Retry}
import com.vpon.ssp.report.dedup.util.Retry.NeedRetryException
import com.vpon.trade.Event

/**
1. read msg from kafka
2. Read the last sent message from kafka. check if the msg is sent before.
3. Save the key of last message from kafka into couchbase if not found.
4. Check couchbase if it has been processed before,
5. If not, send to new kafka topic
6. once ack received then write to couchbase the key of message is sent and offset
Warning: Not consider case of processed messages with multiple batches but has no offset/dedup key in couchbase (maybe removed or lost)
  */

case class ArchiveS3File(key: String, content: Array[Byte])

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

class PartitionActor(val partitionId: Int, val master:ActorRef) extends Actor with ActorLogging with DedupConfig {
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

  private val partitionMetrics = context.actorOf(PartitionMetrics.props(partitionId), s"dedup-metrics-${partitionId}")

  private var currentSourceEpic: Option[CustomizedIterator[Array[Byte], DefaultDecoder]] = None

  private val sourceKafkaParams = Map("metadata.broker.list" -> sourceBrokers)
  private val sourceTopicPartition = TopicAndPartition(sourceTopic, partitionId)

  private val offsetBucketWithKeyPrefix = CBExtension(system).buckets("offset")
  private val offsetBucket          = offsetBucketWithKeyPrefix.bucket
  private val offsetBucketKeyPrefix = offsetBucketWithKeyPrefix.keyPrefix

  private val cbOffsetKey          = s"${offsetBucketKeyPrefix}${partitionId}"

  @volatile
  private var sourceTopicsConsumer: Future[TopicsConsumer[String, Array[Byte], StringDecoder, DefaultDecoder, MessageAndMetadata[String, Array[Byte]]]] = _

  private val awsService = new MockAwsService(
    new AwsConfig(
      regionName = "ap-northeast-1",
      s3BucketName = "ssp-archive",
      dataPrefix = "data/",
      needCompress = false,
      needEncrypt = false
    )
  )
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
        if (log.isDebugEnabled) {
          for (mmd <- mmds) {
            val key = mmd.key()
            val message = mmd.message()
            try {
              val parsedEvent = Event.parseFrom(message)
              log.debug(s"${self.path} ==> [STEP 1.3] received key: $key . Can parse message into Event: ${parsedEvent}.")
            } catch {
              case e: Exception =>
                log.debug(s"${self.path} ==> [STEP 1.3] received key: $key . Can NOT parse message into Event.")
            }
          }
        }
        log.debug(s"${self.path} ==> [STEP 1.4] end handleJobRequest by doWork")
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
        log.debug(s"${self.path} ==> sourceTopicsConsumer failure. ${ExceptionUtils.getStackTrace(e)}")
        val err = s"${self.path} ==> Failed to start PartitionActor for partition $partitionId, so pause work!!\n${ExceptionUtils.getStackTrace(e)}"
        log.error(e, err)
        partitionMetrics ! Error(err)
        self ! PauseWork
    }
  }

  def doWork(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[Any] = {
    val s3files = convert(mmds)
    val f = for {
      sendResult <- send(s3files)
    } yield {
        sendResult match {
          case None => {
            log.debug(s"${self.path} ==> [STEP 6.6] sent failure. But continue to process next batch messages without blocking")
            doPostSendActions(mmds)
          } // all deduped
          case Some(isSentSuccess) => {
            if (isSentSuccess) {
              log.debug(s"${self.path} ==> [STEP 6.6] sent success. continue to doPostSendActions.")
              doPostSendActions(mmds)
            } else {
              log.warning(s"${self.path} ==> [STEP 6.6] sent failure?? But this branch should not be go through!!")
              doPostSendActions(mmds)
            }
          }
        }
      }

    f.recover{
      case e @ (_: KafkaException | _: CouchbaseException) => {
        val time = System.currentTimeMillis()
        log.error(e, e.getMessage)
        partitionMetrics ! Error(e.getMessage)
        log.error(s"${self.path} ==> [STEP 6.6] Caught fatal exception ${e.getClass} when doWork at $time, so pause work!\n${ExceptionUtils.getStackTrace(e)}")
        self ! PauseWork
      }
    }
  }

  private def convert(mmds: List[MessageAndMetadata[String, Array[Byte]]]): List[ArchiveS3File] = {
    if (!mmds.isEmpty) {
      mmds.map(mmd => {
        val eventKey = mmd.key()
        val eventContent = mmd.message()
        val eventOffset = mmd.offset
        val event = Event.parseFrom(eventContent)
        val eventTimestamp = event.`eventType` match {
          case Event.EVENTTYPE.TRADELOG => event.`tradeLog`.get.`bidTimestamp`
          case Event.EVENTTYPE.IMPRESSION => event.`impression`.get.`impressionTimestamp`
          case Event.EVENTTYPE.CLICK => event.`click`.get.`clickTimestamp`
          case _ => {
            val err = s"Unknown EVENTTYPE: ${event.`eventType`}. eventKey: $eventKey. event: ${event.toJson()}"
            log.warning(err)
            partitionMetrics ! UnknownEventType(eventOffset, eventKey)
            0
          }
        }
        val s3FileKey = S3Util.getS3Folder(eventTimestamp, "archive/") + eventKey
        ArchiveS3File(s3FileKey, eventContent)
      })
    } else {
      List.empty[ArchiveS3File]
    }
  }

  private def send(archiveS3Files: List[ArchiveS3File]): Future[Option[Boolean]] = {
    log.debug(s"${self.path} ==> [STEP 6.1] start send")
    if (!archiveS3Files.isEmpty) {
      val sendStartTime = System.currentTimeMillis()
      awsService.send(archiveS3Files, Some(partitionId)).map(k => {
        val sendTime = System.currentTimeMillis() - sendStartTime
        partitionMetrics ! Send(sendTime)
        log.debug(s"${self.path} ==> [STEP 6.5] end send with true result.")
        Some(true)
      })
    } else {
      log.debug(s"${self.path} ==> [STEP 6.5] end send with None result.")
      Future{None}
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
        log.error(s"${self.path} ==> Caught CouchbaseException when doPostSendActions at $time, so pause work!!.\n${ExceptionUtils.getStackTrace(e)}")
        self ! PauseWork
      }
      case e:Throwable => {
        val time = System.currentTimeMillis()
        log.warning(s"${self.path} ==> Caught Throwable when doPostSendActions at $time, but still continue fetch next job.")
        self ! NextJob
      }
    }
  }

  private def updateLastOffset(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[Option[Boolean]] = {
    log.debug(s"${self.path} ==> [STEP 8.1] start updateLastOffset")
    if (!mmds.isEmpty) {
      val lastOffset = mmds.last.offset
      val toSaveOffset = lastOffset + 1
      log.debug(s"${self.path} ==> [STEP 8.2] toSaveOffset: $toSaveOffset")
      retryCouchbaseOperation{
        offsetBucket.upsert(StringDocument.create(cbOffsetKey, toSaveOffset.toString))
      } map {
        strDoc => {
          log.info(s"${self.path} ==> [STEP 8.3] Success to update last offset: $cbOffsetKey -> $toSaveOffset.")
          partitionMetrics ! LastOffset(toSaveOffset)
          log.debug(s"${self.path} ==> [STEP 8.4] end updateLastOffset with true result.")
          Some(true)
        }
      } recover {
        case e: Throwable => {
          val err = s"${self.path} ==> [STEP 8.3] Failed to update last offset: $cbOffsetKey -> $toSaveOffset\n${ExceptionUtils.getStackTrace(e)}"
          throw new CouchbaseException(err)
        }
      }
    } else {
      log.debug(s"${self.path} ==> [STEP 8.2] end updateLastOffset with None result.")
      Future{None}
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
