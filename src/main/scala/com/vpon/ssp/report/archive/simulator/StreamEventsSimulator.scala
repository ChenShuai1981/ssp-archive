package com.vpon.ssp.report.archive.simulator

import _root_.kafka.producer.KeyedMessage
import org.apache.commons.lang3.exception.ExceptionUtils
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.slf4j.LoggerFactory

import com.vpon.ssp.report.archive.kafka.producer.CustomPartitionProducer
import com.vpon.ssp.report.archive.generator.EventGenerator
import com.vpon.trade.Event

object StreamEventsSimulator extends App with GeneratorDrivenPropertyChecks {

  private val logger = LoggerFactory.getLogger(StreamEventsSimulator.getClass)

  import EventGenerator._

  case class ArgumentOptions(brokers: Option[String] = None,
                             topic: Option[String] = None,
                             debug: Option[String] = None)

  val parser = new scopt.OptionParser[ArgumentOptions]("ssp-archive-stream-event-simulator") {
    head("ssp-archive-stream-event-simulator", "0.0.1")
    opt[String]('b', "brokers") action { case (v, c) =>
      c.copy(brokers = Some(v)) } text "Kafka brokers"
    opt[String]('t', "topic") action { case (v, c) =>
      c.copy(topic = Some(v)) } text "Kafka topic"
    opt[String]('d', "debug") action { case (v, c) =>
      c.copy(debug = Some(v)) } text "Show debug"
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
    val showDebug = config.debug.getOrElse("true")
    val edgeEventsProducer = new CustomPartitionProducer[String, Array[Byte]](edgeEventsBrokers, "kafka.serializer.DefaultEncoder", true)
    logger.debug(s"edgeEventsBrokers=${edgeEventsBrokers}, edgeEventsTopic=${edgeEventsTopic}")
    process

    def process:Unit = {
      logger.debug("==> Please input a number which you want to generate events: ")
      val input = scala.io.StdIn.readLine
      while (!input.isEmpty) {
        safeStringToInt(input) match {
          case None => {
            logger.error("Invalid Number! Please input again.")
            process
          }
          case Some(n) => {
            val batchSizeOfGen = 100
            var id: Long = 0L
            var times: Long = -1L
            while (id <= n) {
              times += 1
              val messages = scala.collection.mutable.ListBuffer[KeyedMessage[String, Array[Byte]]]()
              forAll(genEvent) {
                (event: Event) => {
                  id += 1
                  val eventTimestamp = event.`eventType` match {
                    case Event.EVENTTYPE.TRADELOG => event.`tradeLog`.get.`bidTimestamp`
                    case Event.EVENTTYPE.IMPRESSION => event.`impression`.get.`impressionTimestamp`
                    case Event.EVENTTYPE.CLICK => event.`click`.get.`clickTimestamp`
                    case _ => 0
                  }
                  val message = new KeyedMessage[String, Array[Byte]](edgeEventsTopic, eventTimestamp + "_" + event.`eventKey`, event.toByteArray)
                  messages += message
                }
              }
              val actualSize = if (id > n) n % batchSizeOfGen else batchSizeOfGen
              val actualMessages = messages.take(actualSize)
              try {
                if (!actualMessages.isEmpty) edgeEventsProducer.sendMessages(actualMessages)
              } catch {
                case e: Exception => {
                  logger.error(s"***** Failed to send messages *****")
                  logger.error(ExceptionUtils.getStackTrace(e))
                  System.exit(1)
                }
              }
              if (showDebug.eq("true")) {
                for (i <- 0 until actualSize) logger.debug(s"[${(i + 1) + times * batchSizeOfGen}] ==> ${Event.parseFrom(actualMessages(i).message)}")
              }
              if (id >= n) {
                for(k <- 0 until 5) logger.debug("")
                process
              }
            }
          }
        }
      }
    }

    def safeStringToInt(str: String): Option[Int] = try {
      Some(str.toInt)
    } catch {
      case e:NumberFormatException => None
    }
  }
}
