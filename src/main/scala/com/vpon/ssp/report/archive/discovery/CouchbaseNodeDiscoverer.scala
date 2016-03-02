package com.vpon.ssp.report.archive.discovery

import java.net.{InetSocketAddress, Socket}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import akka.actor.{Props, Actor, ActorLogging, Address}
import akka.cluster.Cluster
import com.couchbase.client.java.document.StringDocument
import com.couchbase.client.java.error.DocumentDoesNotExistException
import org.apache.commons.lang3.exception.ExceptionUtils

import com.vpon.ssp.report.archive.couchbase.CBExtension


object CouchbaseNodeDiscoverer {
  def props(selfUuid: String, groupName: String): Props = Props(new CouchbaseNodeDiscoverer(selfUuid, groupName))
  object PublishSelf
  object Join
  object TouchCouchbase
}

//
// Node discovery
//
class CouchbaseNodeDiscoverer(val selfUuid: String, val groupName: String) extends Actor with ActorLogging {

  import CouchbaseNodeDiscoverer._

  val bucket = CBExtension(context.system).buckets("main").bucket
  val keyPrefix = CBExtension(context.system).buckets("main").keyPrefix

  val groupNodeUuidKey = s"nodes-$groupName"
  val selfAddress = Cluster(context.system).selfAddress

  self ! PublishSelf

  implicit val dispatcher = context.dispatcher

  def receive: akka.actor.Actor.Receive = {
    case PublishSelf =>
      publishSelf()

    case Join =>
      joinCluster()

    case TouchCouchbase =>
      storeSelfAddressToCouchbase()

    case _ =>
  }

  private def publishSelf() = {
    log.info(s"Publish self Akka Address $selfAddress")
    val f = for {
      _ <- storeSelfAddressToCouchbase()
      _ <- bucket.insert(StringDocument.create(s"$keyPrefix$groupNodeUuidKey", "")).recover{case _ => true}
      _ <- bucket.append(StringDocument.create(s"$keyPrefix$groupNodeUuidKey", s"|$selfUuid"))
    } yield true
    Await.result(f, 2.seconds)

    log.info(s"publishSelf: key = $keyPrefix$groupNodeUuidKey, content = |$selfUuid")
    log.info("Waiting for other nodes for 2 seconds")
    context.system.scheduler.scheduleOnce(2.seconds, self, Join)
    context.system.scheduler.schedule(30.seconds, 30.seconds, self, TouchCouchbase)
  }

  private def joinCluster() = {
    removeExpiredNode()
    val nodes = collectTargetNodeAddresses()
    log.info(s"Joining cluster. Target nodes: $nodes")
    val cluster = Cluster(context.system)

    if (nodes.isEmpty) {
      log.info(s"Cannot find other node. Join self")
      cluster.joinSeedNodes(immutable.Seq(cluster.selfAddress))
    } else {
      val toJoin = nodes.flatMap { v =>
        val u = v.split('|').toSeq
        if (u.length == 4 && isNodeAvailable(u(2), u(3).toInt)) {
          Some(Address(u.head, u(1), u(2), u(3).toInt))
        } else {
          None
        }
      }.take(3).to[immutable.Seq]

      if (toJoin.isEmpty) {
        log.info(s"Cannot find other node. Join self")
        cluster.joinSeedNodes(immutable.Seq(cluster.selfAddress))
      } else {
        log.info(s"Joining nodes: $toJoin")
        cluster.joinSeedNodes(toJoin)
      }
    }
  }

  private def isNodeAvailable(host: String, port: Int): Boolean = {
    try {
      val socket = new Socket()
      val timeout = 1000
      socket.connect(new InetSocketAddress(host, port), timeout)
      socket.close()
      true
    } catch {
      case e: Throwable => false
    }
  }

  private def storeSelfAddressToCouchbase(): Future[StringDocument] = {
    val k = s"$keyPrefix$selfUuid"
    val v = s"${selfAddress.protocol}|${selfAddress.system}|${selfAddress.host.get}|${selfAddress.port.get}"
    log.debug(s"storeSelfAddressToCouchbase ==> $k -> $v")
    val expiry = 60
    bucket.upsert(StringDocument.create(k, expiry, v))
  }

  private def collectTargetNodeAddresses(): Seq[String] = {

    val nodesF = bucket.get[StringDocument](s"$keyPrefix$groupNodeUuidKey").map(_.content.split('|').filterNot(_.isEmpty).toSeq)
    val result = nodesF.flatMap { nodes =>
      log.info("Nodes in groupNodeUuidKey before 'take' is {}", nodes)
      log.info("selfUuid is {}", selfUuid)
      val targetNodes = nodes.take(math.max(nodes.indexOf(selfUuid), 0))
      log.info("targetNodes is {}", targetNodes)
      Future.traverse(targetNodes) { node =>
        // Handle exceptions here to prevent one document fails, the whole Future fails.
        bucket.get[StringDocument](s"$keyPrefix$node").map(Some.apply).recover {
          // Handle exceptions here to prevent one document fails, the whole Future fails.
          case e: DocumentDoesNotExistException =>
            None
          case e: Exception =>
            log.error("CAN NOT get [{}] due to {}. stacktrace: {}", node, e.getMessage, ExceptionUtils.getStackTrace(e))
            None
        }
      }.map { x =>
        val res = x.flatten.flatMap (_.content.split(';'))
        log.info("targetNodes is {} after read CB.", res)
        res
      }
    } recover {
      case e => Nil
    }
    Await.result(result, 3.seconds)
  }

  private def removeExpiredNode() = {
    val nodesF = bucket.get[StringDocument](s"$keyPrefix$groupNodeUuidKey") map { x =>
      val result = x.content.split('|').filter(_.nonEmpty).toSeq
      log.info("Nodes in groupNodeUuidKey is {}", result)
      result
    }
    val resultF = nodesF flatMap { nodes =>
      Future.traverse(nodes) { node =>
        bucket.get[StringDocument](s"$keyPrefix$node") map {_ => Some(node)} recover {case e => None}
      } map (_.flatten)
    }
    val nodes = Await.result(nodesF, 3.seconds)
    val result = Await.result(resultF, 3.seconds)

    // remove expired nodes uuid
    val toRemove = nodes.toSet -- result.toSet
    if (toRemove.nonEmpty) {
      log.info(s"Remove expired cluster nodes: $toRemove")
      val maxRetries = 10
      val retryIntervalInMillis = 10
      def setUuids(retry: Int): Future[Unit] = {
        def execute = for {
          doc <- bucket.get[StringDocument](s"$keyPrefix$groupNodeUuidKey")
          uuids = doc.content().split('|').filter(v => !v.isEmpty && !toRemove.contains(v)).toSeq
          newValue = uuids.mkString("|", "|", "")
          response <- bucket.replace(StringDocument.create(s"$keyPrefix$groupNodeUuidKey", newValue, doc.cas()))
        } yield ()
        execute.recover {
          case e =>
            Thread.sleep(retryIntervalInMillis)
            if (retry > 0) setUuids(retry - 1) else Future.failed(new Exception("setUuids out of retry attemps"))
        }
      }
      Await.ready(setUuids(maxRetries), 10.seconds)
    }
  }
}
