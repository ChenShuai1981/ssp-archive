package com.vpon.ssp.report.archive

import java.util.UUID

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.pattern.AskTimeoutException

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.exception.ExceptionUtils

import com.vpon.ssp.report.archive.couchbase.CBExtension
import com.vpon.ssp.report.archive.discovery.CouchbaseNodeDiscoverer
import com.vpon.ssp.report.archive.actor._
import com.vpon.ssp.report.archive.config.ArchiveConfig

object Main extends App {

  val DEFAULT_HTTP_PORT = 17661

  case class ArgumentOptions(appConfPath: Option[String] = None,
                    uuid: String = UUID.randomUUID().toString.toUpperCase,
                    httpPort: Int = DEFAULT_HTTP_PORT,
                    akkaPort: Option[Int] = None)

  val parser = new scopt.OptionParser[ArgumentOptions]("ssp-archive-main") {
    head("ssp-archive-main", "0.0.1")
    opt[String]('c', "conf") required() action { case (v, c) =>
      c.copy(appConfPath = Some(v)) } text "specify application.conf path"
    opt[Int]('h', "httpPort") action { case (v, c) =>
      c.copy(httpPort = v) } text "HTTP listening port"
    opt[String]('u', "uuid") action { case (v, c) =>
      c.copy(uuid = v) } text "node uuid"
    opt[Int]('a', "akkaPort") action { case (v, c) =>
      c.copy(akkaPort = Some(v)) } text "HTTP listening port"
    help("help") abbr "?" text "prints this usage text"
  }

  try {
    /**
     * If you set config.resource, config.file, or config.url on-the-fly from inside your program
     * (for example with System.setProperty()), be aware that ConfigFactory has some internal caches
     * and may not see new values for system properties. Use ConfigFactory.invalidateCaches()
     * to force-reload system properties.
     */
    ConfigFactory.invalidateCaches

    parser.parse(args, ArgumentOptions()) match {
      case Some(config) =>
        if (config.appConfPath.isDefined) {
          System.setProperty("config.file", config.appConfPath.get)
        }
        if (config.akkaPort.isDefined) {
          System.setProperty("akka.remote.netty.tcp.port", config.akkaPort.get.toString)
        }
        appStart(config)
      case None => sys.exit(1)
    }
  } catch {
    case ex: Throwable =>
      ExceptionUtils.printRootCauseStackTrace(ex)
      sys.exit(2)
  }

  private def appStart(config: ArgumentOptions) {

    implicit val system = ActorSystem("ssp-archive")
    implicit val dispatcher = system.dispatcher

    //connecting to couchbase which used for storing kafka offset by partitions
    CBExtension(system).buckets("main")

    // init cluster and discovery
    val cluster = Cluster(system)
    val clusterGroup = system.settings.config.getString("ssp-archive.cluster-group")
    val discoverer = system.actorOf(CouchbaseNodeDiscoverer.props(config.uuid, clusterGroup), name= "archive-NodeDiscoverer")

    system.log.info("launched CouchbaseNodeDiscoverer")

    val clusterListener = system.actorOf(SimpleClusterListener.props(), name = "archive-clusterListener")
    cluster.subscribe(clusterListener, classOf[ClusterDomainEvent])

    system.log.info("launched SimpleClusterListener")

    // init actor reaper
    val reaper = system.actorOf(ActorReaper.props(), name = "archive-actorReaper")

    system.log.info("launched ActorReaper")

    // init ServiceBooter
    val booter = system.actorOf(ServiceBooter.props(), name = "archive-serviceBooter")
    booter ! ServiceBooterProtocol.Start(config.httpPort)

    system.log.info("launched ServiceBooter")

    sys addShutdownHook {

      import akka.pattern.gracefulStop
      system.log.info("Shutdown hook caught, start to shutdown service...")

      try {
        system.log.info("Leaving Cluster (selfAddress:{})...", cluster.selfAddress)
        Await.result(gracefulStop(discoverer, 2.minutes), 3.minutes)
      } catch {
        case e: AskTimeoutException =>
      } finally {
        cluster.leave(cluster.selfAddress)
      }

      try {
        system.log.info("Stopping Service...")
        Await.result(gracefulStop(booter, 2.minutes, ServiceBooterProtocol.Stop), 3.minutes)
      } catch {
        case e: AskTimeoutException =>
      }

      try {
        system.log.info("Reaping all actors...")
        Await.result(gracefulStop(reaper, 2.minutes, ActorReaperProtocol.ReapAll), 3.minutes)
      } catch {
        case e : AskTimeoutException =>
      }

      system.log.info("Wait for service termination...")
      system.awaitTermination()
      system.log.info("Shutdown is done.")
    }
  }
}
