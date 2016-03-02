package com.vpon.ssp.report.archive.actor

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.GracefulStopSupport

import com.vpon.ssp.report.archive.util.SequentialExecFuture
import com.vpon.ssp.report.archive.actor.ActorReaperProtocol.{ReapAll, WatchActor}

object ActorReaper {
  def props(): Props = Props(new ActorReaper())
}

object ActorReaperProtocol {
  case class WatchActor(refs: Seq[ActorRef])
  case object ReapAll
}

class ActorReaper extends Actor with ActorLogging with GracefulStopSupport {

  implicit val system = context.system
  implicit val dispatcher = context.dispatcher
  implicit val stopTimeout = 5.minutes
  val watched = ArrayBuffer.empty[ActorRef]

  def receive: akka.actor.Actor.Receive = {
    case WatchActor(refs) =>
      refs foreach context.watch
      watched ++= refs

    case Terminated(ref) =>
      watched -= ref
      if (watched.isEmpty) allReaped()

    case ReapAll => reapAll(watched.toSeq)
  }

  protected def allReaped() {
    log.info("all actors are reaped, system shutdown.")
    context.system.shutdown()
  }

  def reapAll(actors: Seq[ActorRef]) {
    SequentialExecFuture.traverse(actors) { actor =>
      gracefulStop(actor, stopTimeout)
    }
  }
}
