package com.vpon.ssp.report.archive.actor

import akka.actor.{ActorLogging, Actor, Props}
import akka.cluster.ClusterEvent._

object SimpleClusterListener {
  def props(): Props = Props(new SimpleClusterListener())
}

class SimpleClusterListener extends Actor with ActorLogging {
  def receive: akka.actor.Actor.Receive = {
    case state: CurrentClusterState =>
      log.info("Current status: {}, members: {}", state, state.members.mkString(", "))
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) ⇒
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case _: ClusterDomainEvent => // ignore
  }
}
