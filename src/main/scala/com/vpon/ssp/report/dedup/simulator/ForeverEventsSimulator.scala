package com.vpon.ssp.report.dedup.simulator

import _root_.kafka.producer.KeyedMessage
import org.apache.commons.lang3.exception.ExceptionUtils
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.slf4j.LoggerFactory

import com.vpon.ssp.report.dedup.kafka.producer.CustomPartitionProducer
import com.vpon.ssp.report.dedup.generator.EventGenerator
import com.vpon.trade.Event
import com.vpon.trade.Event.EVENTTYPE

object ForeverEventsSimulator extends App with GeneratorDrivenPropertyChecks {

  private val logger = LoggerFactory.getLogger(ForeverEventsSimulator.getClass)

  import EventGenerator._

  case class ArgumentOptions(brokers: Option[String] = None,
                             topic: Option[String] = None,
                             sendInterval: Option[String] = None)

  val parser = new scopt.OptionParser[ArgumentOptions]("ssp-kafka-s3-forever-event-simulator") {
    head("ssp-kafka-s3-forever-event-simulator", "0.0.1")
    opt[String]('b', "brokers") action { case (v, c) =>
      c.copy(brokers = Some(v)) } text "Kafka brokers"
    opt[String]('t', "topic") action { case (v, c) =>
      c.copy(topic = Some(v)) } text "Kafka topic"
    opt[String]('i', "interval") action { case (v, c) =>
      c.copy(sendInterval = Some(v)) } text "send interval"
    help("help") abbr "?" text "prints this usage text"
  }

  try {
    parser.parse(args, ArgumentOptions()) match {
      case Some(config) => appStart(config)
      case None => sys.exit(1)
    }
  } catch {
    case e: Throwable =>
      logger.error(ExceptionUtils.getStackTrace(e))
      sys.exit(2)
  }

  private def appStart(config: ArgumentOptions): Unit = {
    val edgeEventsBrokers = config.brokers.getOrElse("localhost:9092")
    val edgeEventsTopic = config.topic.getOrElse("ssp-edge-events")
    val sendInterval = (config.sendInterval.getOrElse("0")).toLong
    val edgeEventsProducer = new CustomPartitionProducer[String, Array[Byte]](edgeEventsBrokers, "kafka.serializer.DefaultEncoder", true)
    logger.debug(s"edgeEventsBrokers=${edgeEventsBrokers}, edgeEventsTopic=${edgeEventsTopic}")
    process

    def process:Unit = {
      while(true) {
        var batchMessages = new scala.collection.mutable.ListBuffer[KeyedMessage[String, Array[Byte]]]()
        forAll(genEvent) {
          (event: Event) => {
            logger.debug(s"${event}")
            val key = event.`eventType` match {
              case EVENTTYPE.TRADELOG => s"t_${event.`tradeLog`.get.`bidId`}"
              case EVENTTYPE.IMPRESSION => s"i_${event.`impression`.get.`impressionId`}"
              case EVENTTYPE.CLICK => s"c_${event.`click`.get.`clickId`}"
              case _ => ""
            }
            val message = new KeyedMessage[String, Array[Byte]](edgeEventsTopic, key, event.toByteArray)
            batchMessages += message
          }
        }
        try {
          edgeEventsProducer.sendMessages(batchMessages)
        } catch {
          case e: Exception => {
            logger.error(ExceptionUtils.getStackTrace(e))
            System.exit(0)
          }
        }
        if (sendInterval > 0) {
          logger.debug(s"waiting ${sendInterval}ms")
          Thread.sleep(sendInterval)
        }
      }
    }
  }
}
