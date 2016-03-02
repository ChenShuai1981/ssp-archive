package com.vpon.ssp.report.archive.actor.impl

import akka.actor.ActorSelection

object EvenHashing {
  def nodeFor(partitionId: Int, nodes: Array[ActorSelection]): Option[ActorSelection] = {
    if(nodes.isEmpty) {
      None
    } else {
      val index = partitionId % nodes.size
      Some(nodes(index))
    }
  }
}
