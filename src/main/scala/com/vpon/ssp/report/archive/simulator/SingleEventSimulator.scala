package com.vpon.ssp.report.archive.simulator

import scala.collection.mutable.ListBuffer

import _root_.kafka.producer.KeyedMessage
import org.apache.commons.lang3.exception.ExceptionUtils
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.slf4j.LoggerFactory

import com.vpon.ssp.report.archive.kafka.producer.CustomPartitionProducer
import com.vpon.ssp.report.archive.generator.EventGenerator
import com.vpon.trade.Event
import com.vpon.trade.Event.EVENTTYPE

object SingleEventSimulator extends App with GeneratorDrivenPropertyChecks {

  private val logger = LoggerFactory.getLogger(SingleEventSimulator.getClass)

  import EventGenerator._

  case class ArgumentOptions(brokers: Option[String] = None,
                             topic: Option[String] = None)

  val parser = new scopt.OptionParser[ArgumentOptions]("ssp-archive-single-event-simulator") {
    head("ssp-archive-single-event-simulator", "0.0.1")
    opt[String]('b', "brokers") action { case (v, c) =>
      c.copy(brokers = Some(v)) } text "Kafka brokers"
    opt[String]('t', "topic") action { case (v, c) =>
      c.copy(topic = Some(v)) } text "Kafka topic"
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
    val edgeEventsProducer = new CustomPartitionProducer[String, Array[Byte]](edgeEventsBrokers, "kafka.serializer.DefaultEncoder", true)
    logger.debug(s"edgeEventsBrokers=${edgeEventsBrokers}, edgeEventsTopic=${edgeEventsTopic}")
    process

    def process:Unit = {
      val messageList = new ListBuffer[Event]()
      forAll(genEvent) {
        (event: Event) => {
          messageList += event
        }
      }
      logger.debug("Please press any key to generate protobuf message")
      val total = 100
      messageList.take(total).foreach(event => {
        Option(scala.io.StdIn.readLine) match {
          case None =>
          case Some(s) => {
            logger.debug(s"${event}")
            val key = event.`eventType` match {
              case EVENTTYPE.TRADELOG => s"t_${event.`tradeLog`.get.`bidId`}"
              case EVENTTYPE.IMPRESSION => s"i_${event.`impression`.get.`impressionId`}"
              case EVENTTYPE.CLICK => s"c_${event.`click`.get.`clickId`}"
              case _ => ""
            }
            val message = new KeyedMessage[String, Array[Byte]](edgeEventsTopic, key, event.toByteArray)
            try {
              edgeEventsProducer.sendMessages(Seq(message))
            } catch {
              case e: Exception => logger.error(s"failed to send message ${message}")
            }
          }
        }
      })
    }
  }
}
