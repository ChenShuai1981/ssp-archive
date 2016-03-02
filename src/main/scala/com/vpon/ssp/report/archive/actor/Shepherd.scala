package com.vpon.ssp.report.archive.actor

import akka.actor._
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.{Cluster, MemberStatus}

import scala.concurrent.duration._
import scala.language.postfixOps

import com.vpon.ssp.report.archive.kafka.consumer.KafkaCluster
import com.vpon.ssp.report.archive.actor.PartitionMasterProtocol.ReportKafkaConnection
import com.vpon.ssp.report.archive.actor.ShepherdProtocol._
import com.vpon.ssp.report.archive.actor.impl.EvenHashing
import com.vpon.ssp.report.archive.config.ArchiveConfig

/**
 * Cluster singleton that assigns partitions to nodes. It informs all nodes about the current state and changes.
 * The changes may come from Kafka (when partitions are added) or from Akka cluster (when nodes are added or removed).
 */
class Shepherd extends Actor with ActorLogging with ArchiveConfig {

  private[this] val kc = new KafkaCluster(Map("metadata.broker.list" -> sourceBrokers))

  implicit private[this] val ec = context.dispatcher
  private[this] val cluster = Cluster(context.system)
  private[this] val partitionMaster = context.system.actorSelection("/user/archive-partitions")

  override def preStart(): Unit = {
    super.preStart()
    context.system.scheduler.scheduleOnce(clusterRampupTime, self, Initialize)
  }

  /**
   * Behavior that is activated after rump-up period. It actively waits for changes inside cluster and in case
   * of such change it reassigns partitions.
   */
  private[this] val active: Receive = {
    case Refresh =>
      // get current assignments and send them to nodes
      assignments.foreach { case (node, partitions) =>
        node ! AssignedPartitions(partitions)
      }
      context.system.scheduler.scheduleOnce(30 seconds, self, Refresh)

    case _: MemberUp | _: MemberRemoved =>
      // we should wait a little before the cluster state settles down
      context.system.scheduler.scheduleOnce(10 seconds, self, Refresh)

    case GetAssignments => sender() ! assignments
    case GetDistribution => sender() ! distribution
  }

  /**
   * Default behavior that is waiting for initialization of shepherd. Initialization happens after configured
   * rump-up time during which cluster is waiting for other nodes to join. As soon as initialization is triggered,
   * shepherd starts to actively manage nodes.
   */
  private[this] val warmup: Receive = {
    case Initialize =>
      cluster.subscribe(self, classOf[MemberEvent])
      context become active
      self ! Refresh
  }

  def receive: akka.actor.Actor.Receive = warmup

  /**
   * Obtains current partition number from Kafka.
   * @return number of partitions
   */
  private def partitionIds: Iterable[Int] = {
    kc.getPartitionMetadata(Set(sourceTopic)) match {
      case Left(err) =>
        log.error("partitionIds => " + err.toString())
        partitionMaster ! ReportKafkaConnection(sourceBrokers, sourceTopic, false)
        Iterable.empty[Int]
      case Right(tm) => {
        partitionMaster ! ReportKafkaConnection(sourceBrokers, sourceTopic, true)
        tm.flatMap(_.partitionsMetadata.map(_.partitionId))
      }
    }
  }

  /**
   * Returns number of assigned partitions for each of nodes.
   * @return map of node → number of partitions
   */
  private def distribution: Distribution = assignments.toDistribution

  /**
   * Returns current (node, partition) assignments.
   * @return map of node → partitions
   */
  private[this] def assignments: Assignments = {
    // obtain current living nodes (as cluster members) and transform into ActorSelection (PartitionMaster)
    val nodes = cluster.state.members
      .filter(_.status == MemberStatus.up)
      .map(m => context.actorSelection(RootActorPath(m.address) / "user" / "archive-partitions"))

    // find which partitions are assigned to which nodes
    val assignmentMap: Map[ActorSelection, Set[Int]] = partitionIds.flatMap { p =>
      EvenHashing.nodeFor(p, nodes.toArray).map(n => (p, n))
    }.groupBy(_._2).mapValues(_.map(_._1).toSet)

    Assignments(assignmentMap)
  }

}

object ShepherdProtocol {
  case class AssignedPartitions(partitionId: Set[Int])
  case object Refresh
  case object Initialize
  case object GetAssignments
  case class Assignments(assignments: Map[ActorSelection, Set[Int]]) {
    def foreach: (((ActorSelection, Set[Int])) => Any) => Unit = assignments.foreach _
    def toDistribution: Distribution = Distribution(assignments.mapValues(_.size))
  }
  case object GetDistribution
  case class Distribution(distribution: Map[ActorSelection, Int])
}

object Shepherd {
  def props: Props = Props[Shepherd]
}
