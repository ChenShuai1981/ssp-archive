package com.vpon.ssp.report.dedup.actor

import scala.collection.mutable.ArrayBuffer

import akka.actor._
import akka.contrib.pattern.ClusterSingletonManager
import akka.io.IO
import spray.can.Http

import com.vpon.ssp.report.dedup.actor.ActorReaperProtocol.WatchActor
import com.vpon.ssp.report.dedup.actor.ServiceBooterProtocol.{Stop, Start}
import com.vpon.ssp.report.dedup.config.DedupConfig

object ServiceBooter {
  def props(): Props = Props(new ServiceBooter())
}

object ServiceBooterProtocol {
  case class Start(port: Int)
  case object Stop
}

class ServiceBooter() extends Actor with ActorLogging with DedupConfig {

  implicit val dispatcher = context.dispatcher
  val system = context.system
  val reaper: ActorSelection = system.actorSelection("/user/dedup-actorReaper")

  private def become(receive: Receive) = context.become(receive orElse unknown)

  private val unknown: Receive = {
    case msg => log.info("ssp-kafka-s3 ServiceBooter got unknown message: {}", unknown)
  }

  private val stopped: Receive = {
    case Start(port) =>
      log.info("ssp-kafka-s3.start")
      become(starting)
      initActors(port)
  }

  def receive: akka.actor.Actor.Receive = stopped orElse unknown

  private def starting: Receive = {
    case Http.Bound(address) =>
      log.info("ssp-kafka-s3.Bound. address: {}", address)
      become(started(sender()))
  }

  private def started(listener: ActorRef): Receive = {
    case Stop =>
      log.info("ssp-kafka-s3.Stop")
      become(stopping)
      context.watch(listener)
      listener ! Http.Unbind
  }

  private def stopping: Receive = {
    case Http.Unbound =>
      log.info("ssp-kafka-s3.Unbound")

    case Terminated(_) =>
      log.info("ssp-kafka-s3.Terminated")
      context.stop(self)
  }

  private def initActors(port: Int) {

    val actors = ArrayBuffer.empty[ActorRef]

    context.system.actorSelection("../")

    actors += system.actorOf(PartitionMaster.props(), name = "dedup-partitions")

    system.actorOf(ClusterSingletonManager.props(
      singletonProps = Shepherd.props,
      singletonName = "dedup-shepherd",
      terminationMessage = PoisonPill,
      role = None
    ), "dedup-shepherd")

    val webServiceActor = system.actorOf(WebServiceActor.props(), name = "dedup-webService")
    actors += webServiceActor
    IO(Http)(system) ! Http.Bind(webServiceActor, interface = "0.0.0.0", port = port)

    reaper ! WatchActor(actors.toSeq)
  }
}
