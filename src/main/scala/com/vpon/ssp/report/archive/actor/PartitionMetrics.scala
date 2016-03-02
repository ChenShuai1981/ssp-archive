package com.vpon.ssp.report.archive.actor

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import akka.actor.{Actor, ActorLogging, Props}
import akka.util.Timeout

import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

import com.vpon.ssp.report.archive.actor.PartitionMetricsProtocol._

object PartitionMetricsProtocol {

  case object GetMetrics

  case object GetInfo

  case object Refresh

  case class Warning(warning: String)

  case class Error(error: String)

  case class Consume(offset: Long, key: String)

  case class Send(time: Double)

  case class LastOffset(offset: Long)

  case class IsWorking(isWorking: Boolean)

  case object Reset

  case object Resume

  case class UnParsedEvent(offset: Long, key: String)

  case class UnknownEventType(offset: Long, key: String)

  case class InvalidEvent(offset: Long, key: String)

  case class CouchbaseError(offset: Long, key: String)

  case class UnknownError(offset: Long, key: String)

}

object PartitionMetrics {

  def akkaMBeansStatsPresentation(): JsObject = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    val clusterObjName = new ObjectName("akka:type=Cluster")
    JsObject("Available" -> JsString(mbeanServer.getAttribute(clusterObjName, "Available").toString),
      "ClusterStatus" -> JsonParser(mbeanServer.getAttribute(clusterObjName, "ClusterStatus").toString),
      "Leader" -> JsString(mbeanServer.getAttribute(clusterObjName, "Leader").toString),
      "MemberStatus" -> JsString(mbeanServer.getAttribute(clusterObjName, "MemberStatus").toString),
      "Members" -> JsString(mbeanServer.getAttribute(clusterObjName, "Members").toString),
      "Unreachable" -> JsString(mbeanServer.getAttribute(clusterObjName, "Unreachable").toString)
    )
  }

  def jvmMBeansStatsPresentation(): JsObject = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean
    val threadMXBean = ManagementFactory.getThreadMXBean
    val memoryMXBean = ManagementFactory.getMemoryMXBean

    JsObject("HeapMemoryUsage_init" -> JsNumber(memoryMXBean.getHeapMemoryUsage.getInit.toString),
      "HeapMemoryUsage_used" -> JsNumber(memoryMXBean.getHeapMemoryUsage.getUsed.toString),
      "NonHeapMemoryUsage_init" -> JsNumber(memoryMXBean.getNonHeapMemoryUsage.getInit.toString),
      "NonHeapMemoryUsage_used" -> JsNumber(memoryMXBean.getNonHeapMemoryUsage.getUsed.toString),
      "SystemLoadAverage" -> JsNumber(osMXBean.getSystemLoadAverage.toString),
      "PeakThreadCount" -> JsNumber(threadMXBean.getPeakThreadCount.toString),
      "ThreadCount" -> JsNumber(threadMXBean.getThreadCount.toString)
    )
  }

  def props(partitionId: Int): Props = {
    Props(new PartitionMetrics(partitionId))
  }

}

class PartitionMetrics(val partitionId: Int) extends Actor with ActorLogging {
  import PartitionMetrics._

  implicit val dispatcher = context.system.dispatcher

  implicit val timeout = Timeout(5 seconds)

  private var error = ""
  private var warning = ""

  private val newPercentage = 0.2
  private val oldPercentage = 1 - newPercentage

  private var totalSends = 0L
  private var avgSendsPerSec = 0d
  private var accSends = 0L
  private var avgSendTime = 0d

  private var totalConsumes = 0L
  private var avgConsumesPerSec = 0d
  private var accConsumes = 0L

  private var lastOffset = 0L

  private var isWorking = true

  private var totalCouchbaseErrors = 0L
  private var avgCouchbaseErrorsPerSec = 0d
  private var accCouchbaseErrors = 0L

  private var totalUnknownErrors = 0L
  private var avgUnknownErrorsPerSec = 0d
  private var accUnknownErrors = 0L

  private var totalUnParsedEvents = 0L
  private var avgUnParsedEventsPerSec = 0d
  private var accUnParsedEvents = 0L

  private var totalUnknownEventTypes = 0L
  private var avgUnknownEventTypesPerSec = 0d
  private var accUnknownEventTypes = 0L

  private var totalInvalidEvents = 0L
  private var avgInvalidEventsPerSec = 0d
  private var accInvalidEvents = 0L

  private var lastCalAvgTimestamp = System.currentTimeMillis

  log.debug(s"${self.path} ==> PartitionMetrics is up")

  context.system.scheduler.schedule(0.second, 1.second, new Runnable {
    override def run() {
      self ! Refresh
    }
  })

  def receive: akka.actor.Actor.Receive = {

    case Resume =>
      log.debug(s"${self.path} ===> Resume")
      error = ""
      warning = ""
      isWorking = true

    case Reset =>
      log.debug(s"${self.path} ===> Reset")

      totalConsumes = 0L
      avgConsumesPerSec = 0d
      accConsumes = 0L

      totalCouchbaseErrors = 0L
      avgCouchbaseErrorsPerSec = 0d
      accCouchbaseErrors = 0L

      totalUnknownErrors = 0L
      avgUnknownErrorsPerSec = 0d
      accUnknownErrors = 0L

      totalUnParsedEvents = 0L
      avgUnParsedEventsPerSec = 0d
      accUnParsedEvents = 0L

      totalUnknownEventTypes = 0L
      avgUnknownEventTypesPerSec = 0d
      accUnknownEventTypes = 0L

      totalInvalidEvents = 0L
      avgInvalidEventsPerSec = 0d
      accInvalidEvents = 0L

      totalSends = 0L
      avgSendsPerSec = 0d
      accSends = 0L
      avgSendTime = 0d

      lastCalAvgTimestamp = System.currentTimeMillis

      lastOffset = 0L
      error = ""
      warning = ""
      isWorking = true

    case Consume(offset, key) =>
      log.debug(s"${self.path} ===> Consume (offset = $offset, key = $key)")
      totalConsumes += 1
      calculateAverage(1, 0, 0, 0, 0, 0, 0)

    case CouchbaseError(offset, key) =>
      log.debug(s"${self.path} ===> CouchbaseError (offset = $offset, key = $key)")
      totalCouchbaseErrors += 1
      calculateAverage(0, 1, 0, 0, 0, 1, 0)

    case UnknownError(offset, key) =>
      log.debug(s"${self.path} ===> UnknownError (offset = $offset, key = $key)")
      totalUnknownErrors += 1
      calculateAverage(0, 0, 1, 0, 0, 0, 1)

    case UnParsedEvent(offset, key) =>
      log.debug(s"${self.path} ===> UnParsedEvent (offset = $offset, key = $key)")
      totalUnParsedEvents += 1
      calculateAverage(0, 0, 0, 1, 0, 0, 0)

    case UnknownEventType(offset, key) =>
      log.debug(s"${self.path} ===> UnknownEventType (offset = $offset, key = $key)")
      totalUnknownEventTypes += 1
      calculateAverage(0, 0, 0, 0, 1, 0, 0)

    case InvalidEvent(offset, key) =>
      log.debug(s"${self.path} ===> InvalidEvent (offset = $offset, key = $key)")
      totalInvalidEvents += 1
      calculateAverage(0, 0, 0, 0, 0, 1, 0)

    case Send(time) =>
      log.debug(s"${self.path} ===> Send (time = $time)")
      totalSends += 1
      avgSendTime = movingAverage(avgSendTime, time)
      calculateAverage(0, 0, 0, 0, 0, 0, 1)

    case Refresh =>
      calculateAverage(0, 0, 0, 0, 0, 0, 0)

    case Error(err: String) if (!err.isEmpty) =>
      log.debug(s"${self.path} ===> Error")
      error = "%d => %s".format(System.currentTimeMillis, err)

    case Warning(warn: String) if (!warn.isEmpty) =>
      log.debug(s"${self.path} ===> Warning")
      warning = "%d => %s".format(System.currentTimeMillis, warn)

    case LastOffset(offset) =>
      log.debug(s"${self.path} ===> LastOffset (offset = $offset)")
      lastOffset = offset

    case IsWorking(working) =>
      log.debug(s"${self.path} ===> IsWorking ($working)")
      isWorking = working

    case GetMetrics =>
      log.debug(s"${self.path} ===> GetMetrics")
      sender() ! metricsStatsPresentation()

    case GetInfo =>
      log.debug(s"${self.path} ===> GetInfo")
      sender() ! infoStatsPresentation()
  }

  private def metricsStatsPresentation() = {
    JsObject(
      "partitionId" -> JsNumber(partitionId),
      "error" -> JsString(error),
      "warning" -> JsString(warning),
      "is_working" -> JsBoolean(isWorking),
      "last_offset" -> JsNumber(lastOffset),
      "total" -> JsObject(
        "consumes" -> JsNumber(totalConsumes),
        "couchbaseErrors" -> JsNumber(totalCouchbaseErrors),
        "unknownErrors" -> JsNumber(totalUnknownErrors),
        "unParsedEvents" -> JsNumber(totalUnParsedEvents),
        "unknownEventTypes" -> JsNumber(totalUnknownEventTypes),
        "invalidEvents" -> JsNumber(totalInvalidEvents),
        "sends" -> JsNumber(totalSends)
      ),
      "average" -> JsObject(
        "avg_send_time" -> JsNumber(avgSendTime),
        "consumes_per_second" -> JsNumber(avgConsumesPerSec),
        "couchbase_errors_per_second" -> JsNumber(avgCouchbaseErrorsPerSec),
        "unknown_errors_per_second" -> JsNumber(avgUnknownErrorsPerSec),
        "unparsed_events_per_second" -> JsNumber(avgUnParsedEventsPerSec),
        "unknown_event_types_per_second" -> JsNumber(avgUnknownEventTypesPerSec),
        "invalid_events_per_second" -> JsNumber(avgInvalidEventsPerSec),
        "send_per_second" -> JsNumber(avgSendsPerSec)
      )
    )
  }

  private def calculateAverage(
    consumeIncrementor: Int,
    couchbaseErrorIncrementor: Int,
    unknownErrorIncrementor: Int,
    unParsedEventIncrementor: Int,
    unknownEventTypeIncrementor: Int,
    invalidEventIncrementor: Int,
    sendIncrementor: Int) = {

    val now = System.currentTimeMillis
    val timeDiffInSec = (now - lastCalAvgTimestamp) / 1000.0
    if (timeDiffInSec > 1) {
      lastCalAvgTimestamp = now
      avgConsumesPerSec = movingAverage(avgConsumesPerSec, accConsumes / timeDiffInSec)
      avgCouchbaseErrorsPerSec = movingAverage(avgCouchbaseErrorsPerSec, accCouchbaseErrors / timeDiffInSec)
      avgUnknownErrorsPerSec = movingAverage(avgUnknownErrorsPerSec, accUnknownErrors / timeDiffInSec)
      avgUnParsedEventsPerSec = movingAverage(avgUnParsedEventsPerSec, accUnParsedEvents / timeDiffInSec)
      avgUnknownEventTypesPerSec = movingAverage(avgUnknownEventTypesPerSec, accUnknownEventTypes / timeDiffInSec)
      avgInvalidEventsPerSec = movingAverage(avgInvalidEventsPerSec, accInvalidEvents / timeDiffInSec)
      avgSendsPerSec = movingAverage(avgSendsPerSec, accSends / timeDiffInSec)

      accConsumes = 0
      accCouchbaseErrors = 0
      accUnknownErrors = 0
      accUnParsedEvents = 0
      accUnknownEventTypes = 0
      accInvalidEvents = 0
      accSends = 0
    } else {
      accConsumes += consumeIncrementor
      accCouchbaseErrors += couchbaseErrorIncrementor
      accUnknownErrors += unknownErrorIncrementor
      accUnParsedEvents += unParsedEventIncrementor
      accUnknownEventTypes += unknownEventTypeIncrementor
      accInvalidEvents += invalidEventIncrementor
      accSends += sendIncrementor
    }
  }

  private def movingAverage(acc: Double, adding: Double) = (acc * oldPercentage) + (adding * newPercentage)

  private[this] def infoStatsPresentation() = {
      JsObject(
        "akka" -> akkaMBeansStatsPresentation,
        "jvm" -> jvmMBeansStatsPresentation,
        "metrics" -> metricsStatsPresentation)
  }

}

