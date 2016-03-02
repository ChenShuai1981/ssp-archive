package com.vpon.ssp.report.archive.actor

import akka.actor._

import scala.concurrent.duration._

import com.vpon.ssp.report.archive.actor.PartitionMasterProtocol.{PartitionStat, GetKafkaConnection, ReportKafkaConnection, ReportPartitionStat}
import com.vpon.ssp.report.archive.actor.ShepherdProtocol.AssignedPartitions
import com.vpon.ssp.report.archive.config.ArchiveConfig


class PartitionMaster() extends Actor with ActorLogging with ArchiveConfig {
    import context._
    private var kafkaBrokers = ""
    private var kafkaTopic = ""
    private var isKafkaConnectOK = false

    def receive: akka.actor.Actor.Receive = {

        case AssignedPartitions(partitions) => managePartitions(partitions)

        case ReportPartitionStat => reportPartitionStat()

        case ReportKafkaConnection(kafkaBrokers, kafkaTopic, isKafkaConnectOK) => reportKafkaConnection(kafkaBrokers, kafkaTopic, isKafkaConnectOK)

        case GetKafkaConnection => getKafkaConnection()

        case Terminated(partitionActor) =>
            log.info(s"PartitionActor $partitionActor got terminated.")

        case _ =>
    }

    /**
     * Manages partitions using current state and aggregated `AssignedPartition` messages. It does three things:
     *
     * 1. creates new `PartitionActor` for newly assigned partitions
     * 2. stops `PartitionActor`s for which assignments were removed
     * 3. keeps the rest of `PartitionActor`s running
     *
     * Beware, it's very impure function, don't get dirty.
     */
    private[this] def managePartitions(assigned: Set[Int]) = {

        log.debug(s"current node assigned partitions: ${assigned}")

        def logPartitionsChange[T : Ordering](action: String, set: TraversableOnce[T]) = {
            log.debug("Partitions {}: {}", action, set.toSeq.sorted.mkString("[", ", ", "]"))
        }

        log.debug(s"=====> assigned: $assigned")
        val current = activePartitions
        log.debug(s"=====> current: $current")

        val toIgnore = assigned.intersect(current)
        log.debug(s"=====> toIgnore: $toIgnore")
        val toRemove = current -- toIgnore
        log.debug(s"=====> toRemove: $toRemove")
        val toCreate = assigned -- toIgnore
        log.debug(s"=====> current: $current")

        toRemove.flatMap(actorForPartition).foreach(context.stop)
        toCreate.foreach { partitionId =>
            log.debug(s"=====> creating PartitionActor: archive-${partitionId.toString}")
            val partitionActor = context.actorOf(PartitionActor.props(partitionId, self), s"archive-${partitionId.toString}")
            watch(partitionActor)
        }

        logPartitionsChange("previously watched", current)
        logPartitionsChange("to be unassigned", toRemove)
        logPartitionsChange("to be newly assigned", toCreate)
        logPartitionsChange("to be left alone", toIgnore)
        logPartitionsChange("newly watched", toIgnore ++ toCreate)

    }

    /**
     * Returns currently assigned partitions.
     * @return list of currently assigned partitions
     */
    private[this] def activePartitions: Set[Int] = context.children.map(_.path.name.split("-").last.toInt).toSet

    /**
     * Finds an `ActorRef` for a given partition.
     * @param partition partition id
     * @return `ActorRef` of actor that is responsible for given partition
     */
    private[this] def actorForPartition(partition: Int): Option[ActorRef] = context.child(partition.toString)

    private[this] def reportPartitionStat() = {
        sender() ! PartitionStat(activePartitions)
    }

    private[this] def reportKafkaConnection(kafkaBrokers: String, kafkaTopic: String, isKafkaConnectOK: Boolean) = {
        this.isKafkaConnectOK = isKafkaConnectOK
        this.kafkaBrokers = kafkaBrokers
        this.kafkaTopic = kafkaTopic
    }

    private[this] def getKafkaConnection() = {
        val status = if (isKafkaConnectOK) "OK" else "Broken"
        val message = s"Kafka connection is $status, brokers is $kafkaBrokers, topic is $kafkaTopic"
        sender() ! message
    }

    val maxRetries = 10
    val timeRange = 1.minute
    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = maxRetries, withinTimeRange = timeRange) {
        case ex: Exception =>
            log.error("Actor died: {}",ex)
            SupervisorStrategy.Escalate
    }
}

object PartitionMasterProtocol {
    case class PartitionStat(activePartitions: Set[Int])
    case object ReportPartitionStat
    case object GetKafkaConnection
    case class ReportKafkaConnection(brokers: String, topic: String, isConnectOK: Boolean)
}

object PartitionMaster {
    def props(): Props = Props(new PartitionMaster())
}
