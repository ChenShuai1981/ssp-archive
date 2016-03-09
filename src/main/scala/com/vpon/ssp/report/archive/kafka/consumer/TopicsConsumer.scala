// scalastyle:off
package com.vpon.ssp.report.archive.kafka.consumer

import java.nio.channels.ClosedChannelException
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import TopicsConsumer.EndOffset
import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common.{NotLeaderForPartitionException, LeaderNotAvailableException, TopicAndPartition, ErrorMapping}
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndOffset, MessageAndMetadata}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.commons.lang3.exception.ExceptionUtils
import scala.annotation.tailrec

import scala.reflect.{ClassTag, classTag}
import scala.util.{Failure, Success, Try}


/** @param topic kafka topic name
  * @param partition kafka partition id
  * @param fromOffset inclusive starting offset
  * @param untilOffset exclusive ending offset. 0 == unlimited
  * @param host preferred kafka host, i.e. the leader at the time the rdd was created
  * @param port preferred kafka host's port
  */
class KafkaPartition(val index: Int,
                     val topic: String,
                     val partition: Int,
                     val fromOffset: Long,
                     val untilOffset: Long,
                     val host: String,
                     val port: Int)

class TopicsConsumer[K: ClassTag,
V: ClassTag,
U <: Decoder[_]: ClassTag,
T <: Decoder[_]: ClassTag,
R: ClassTag] (
               val kc: KafkaCluster,
               val kafkaPartitions: Array[KafkaPartition],
               messageHandler: MessageAndMetadata[K, V] => R) extends NextIterator[R] {

  //log.info(s"Computing topic ${part.topic}, partition ${part.partition} " +
  //  s"offsets ${part.fromOffset} -> ${part.untilOffset}"

  private[this] val buffer = new java.util.concurrent.ArrayBlockingQueue[R](10000)

  private[this] def createPartitionConsumerThread(p: PartitionConsumer) = new Thread(new Runnable {
    def run() {
      @tailrec
      def startConsumer(): Unit = {
        Try {
          p foreach buffer.put
        } match {
          case Success(w) => //system.log.warning(s"Consumer thread stopped for ${tc.kafkaPartitions}}}")
          case Failure(e) =>
            //system.log.error(e,s"Consumer thread for ${tc.kafkaPartitions} crashed. Restarting.")
            Thread.sleep(5000)
            startConsumer()
        }
      }
      startConsumer()
    }
  })

  private val partitionConsumers = kafkaPartitions.map(new PartitionConsumer(_))
  private val partitionConsumerThreads = partitionConsumers.map(createPartitionConsumerThread)



  def isClosed = this.closed

  def close() = {
    partitionConsumers.foreach(_.closeIfNeeded())
    partitionConsumerThreads.foreach(_.join(30000))
  }

  assert(partitionConsumers.length > 0, "There has to be at least one partition to consume")

  var ci = -1

  override def getNext(): R = {
    var result = buffer.poll()
    var retry = 0
    while(result == null && !finished) {
      retry += 1
      Thread.sleep(Math.min(retry * 50, 5000))
      val finished = partitionConsumers.forall(_.isFinished)
      result = buffer.poll()
      if(finished && (result == null)) {
        this.finished = true
        this.closeIfNeeded()
      }
    }
    result
  }

  def getNextIfAvailable(): Option[R] = {
    val result = buffer.poll(5, TimeUnit.SECONDS)
    val finished = partitionConsumers.forall(_.isFinished)
    if(finished && (result == null)) {
      this.finished = true
      this.closeIfNeeded()
    }
    if (result == null) {
      None
    } else {
      Some(result)
    }
  }

  def stream: Stream[R] =  this.toStream



  val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
    .newInstance(kc.config.props)
    .asInstanceOf[Decoder[K]]
  val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
    .newInstance(kc.config.props)
    .asInstanceOf[Decoder[V]]

  partitionConsumerThreads.foreach(_.start())


  /*
    val topicpartitions = offsetRanges.map(_.toTopicAndPartition)

    @tailrec
    protected final def latestLeaderOffsets(retries: Int): Map[TopicAndPartition, LeaderOffset] = {
      val o = kc.getLatestLeaderOffsets(topicpartitions)
      // Either.fold would confuse @tailrec, do it manually
      if (o.isLeft) {
        val err = o.left.get.toString
        if (retries <= 0) {
          throw new Exception(err)
        } else {
          //log.error(err)
          Thread.sleep(kc.config.refreshLeaderBackoffMs)
          latestLeaderOffsets(retries - 1)
        }
      } else {
        o.right.get
      }
    }
  */
  private class PartitionConsumer(val part: KafkaPartition) extends NextIterator[R] {

    var requestOffset = part.fromOffset

    var iter: Iterator[MessageAndOffset] = null

    private[this] var closed = false
    def isFinished = {
      this.finished
    }

    // The idea is to use the provided preferred host, except on task retry atttempts,
    // to minimize number of kafka metadata requests
    @tailrec
    private def connectLeader: SimpleConsumer = {
      if (3 > 0) {
        val leader = kc.connectLeader(part.topic, part.partition)
        if(leader.isRight) leader.right.get
        else {
          Thread.sleep(kc.config.refreshLeaderBackoffMs)
          connectLeader
        }
      } else {
        kc.connect(part.host, part.port)
      }
    }

    var consumer = connectLeader

    private def handleFetchErr(resp: FetchResponse) {
      if (resp.hasError) {
        val err = resp.errorCode(part.topic, part.partition)
        if (err == ErrorMapping.LeaderNotAvailableCode ||
          err == ErrorMapping.NotLeaderForPartitionCode) {
          //log.error(s"Lost leader for topic ${part.topic} partition ${part.partition}, " +
          //  s" sleeping for ${kc.config.refreshLeaderBackoffMs}ms")
        }
        // Let normal rdd retry sort out reconnect attempts
        throw ErrorMapping.exceptionFor(err)
      }
    }

    @tailrec
    private[this] def fetchBatch: Iterator[MessageAndOffset] = {
      try {
        val req = new FetchRequestBuilder()
          .addFetch(part.topic, part.partition, requestOffset, kc.config.fetchMessageMaxBytes)
          .build()
        val resp = consumer.fetch(req)
        handleFetchErr(resp)
        // kafka may return a batch that starts before the requested offset
        resp.messageSet(part.topic, part.partition)
          .iterator
          .dropWhile(_.offset < requestOffset)
      } catch {
        case e: ClosedChannelException =>
          consumer = connectLeader
          fetchBatch
        case e: LeaderNotAvailableException =>
          consumer = connectLeader
          fetchBatch
        case e: NotLeaderForPartitionException =>
          consumer = connectLeader
          fetchBatch
      }
    }

    def close(): Unit = {
      consumer.close()
      closed = true
    }

    private def errBeginAfterEnd(part: KafkaPartition): String =
      s"Beginning offset ${part.fromOffset} is after the ending offset ${part.untilOffset} " +
        s"for topic ${part.topic} partition ${part.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged"

    private def errRanOutBeforeEnd(part: KafkaPartition): String =
      s"Ran out of messages before reaching ending offset ${part.untilOffset} " +
        s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
        " This should not happen, and indicates that messages may have been lost"

    private def errOvershotEnd(itemOffset: Long, part: KafkaPartition): String =
      s"Got $itemOffset > ending offset ${part.untilOffset} " +
        s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
        " This should not happen, and indicates a message may have been skipped"


    private[this] def createMessage(item: MessageAndOffset) = {
      requestOffset = item.nextOffset
      messageHandler(new MessageAndMetadata(
        part.topic, part.partition, item.message, item.offset, keyDecoder, valueDecoder))
    }

    override protected def getNext(): R = {
      var fetchretry = 0
      while((iter == null || !iter.hasNext) && !closed && (!(requestOffset >= part.untilOffset) || part.untilOffset == EndOffset.UNLIMITED)) {
        iter = fetchBatch
        fetchretry += 1
        if(!iter.hasNext) Thread.sleep(Math.min(fetchretry * 50, 5000)) // Wait a bit if there are no new messages in the partition
      }
      if(iter != null && iter.hasNext) {
        val item = iter.next()
        if (item.offset >= part.untilOffset && part.untilOffset > 0) {
          assert(item.offset == part.untilOffset, errOvershotEnd(requestOffset, part))
          this.finished = true
          null.asInstanceOf[R]
        } else {
          createMessage(item)
        }
      } else {
        closeIfNeeded()
        this.finished = true
        null.asInstanceOf[R]
      }
    }
  }
}

object TopicsConsumer {

  private def getPartitions(offsetRanges: Array[OffsetRange]): Array[KafkaPartition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
      new KafkaPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset, "", 0)
    }.toArray
  }




  /*
  def apply(kafkaParams: Map[String, String], topics: Set[String]) = {
    val kc = new KafkaCluster(kafkaParams)
    val topicsAndPartitions = kc.getPartitions(topics)
    val leaders = topicsAndPartitions.fold(err => Left(err), tp => kc.findLeaders(tp))
  }

  def apply(kafkaParams: Map[String, String], topicpartitions: Set[TopicAndPartition]) = {
    val kc = new KafkaCluster(kafkaParams)
  }
  */

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kc: KafkaCluster, topicpartitions: Set[TopicAndPartition], ro: RelativeOffset, eof: EndOffset) = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => mmd
    val earliest = kc.getEarliestLeaderOffsets(topicpartitions).right.getOrElse(Map.empty)
    val latest = kc.getLatestLeaderOffsets(topicpartitions).right.getOrElse(Map.empty)
    val combined = earliest.map {
      case (k,v) => (k, (v,latest.getOrElse(k, {throw new Exception(s"key $k exists in earliest but missing in latest offsets")})))
    }
    val partitions = for {
      (tp,(eo,lo)) <- combined

    } yield {
        val fromOffset = ro.o match {
          case o if o <= 0 && (lo.offset + o) >= eo.offset => lo.offset + o
          case o if o <= 0 && (lo.offset + o) < eo.offset => eo.offset
          case o if o > 0 && (eo.offset + o) > lo.offset => lo.offset
          case o if o > 0 && (eo.offset + o) <= lo.offset => eo.offset + o
        }
        assert(eo.host == lo.host)
        assert(eo.port == lo.port)
        val endoffset = if(eof.fromCurrentLatest) lo.offset else eof.o
        new KafkaPartition(0, tp.topic, tp.partition, fromOffset, endoffset, eo.host, eo.port)
      }
    new TopicsConsumer[K,V,KD,VD,MessageAndMetadata[K, V]](kc, partitions.toArray, messageHandler)
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kc: KafkaCluster, topicpartitions: Set[TopicAndPartition], ao: AbsoluteOffset, eof: EndOffset) = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => mmd
    val earliest = kc.getEarliestLeaderOffsets(topicpartitions).right.getOrElse(Map.empty)
    val latest = kc.getLatestLeaderOffsets(topicpartitions).right.getOrElse(Map.empty)
    val combined = earliest.map {
      case (k,v) => (k, (v,latest.getOrElse(k, {throw new Exception(s"key $k exists in earliest but missing in latest offsets")})))
    }
    val partitions = for {
      (tp, (eo, lo)) <- combined
    } yield {
        val fromOffset = ao.o match {
          case o if o > lo.offset => lo.offset
          case o if o < eo.offset => eo.offset
          case o => o
        }
        assert(eo.host == lo.host)
        assert(eo.port == lo.port)
        val endoffset = if(eof.fromCurrentLatest) lo.offset else eof.o
        new KafkaPartition(0, tp.topic, tp.partition, fromOffset, endoffset, eo.host, eo.port)
      }
    new TopicsConsumer[K,V,KD,VD,MessageAndMetadata[K,V]](kc, partitions.toArray, messageHandler)
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kafkaParams: Map[String, String], topicpartitions: Set[TopicAndPartition], ao: AbsoluteOffset,
   eof: EndOffset): TopicsConsumer[K,V,KD,VD, MessageAndMetadata[K,V]] = {
    val kc = new KafkaCluster(kafkaParams)
    TopicsConsumer[K,V,KD,VD](kc, topicpartitions, ao, eof)
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kafkaParams: Map[String, String], topicpartitions: Set[TopicAndPartition], ao: AbsoluteOffset)
  : TopicsConsumer[K,V,KD,VD, MessageAndMetadata[K,V]] = {
    val kc = new KafkaCluster(kafkaParams)
    TopicsConsumer[K,V,KD,VD](kc, topicpartitions, ao, EndOffset(EndOffset.UNLIMITED))
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kafkaParams: Map[String, String], topicpartitions: Set[TopicAndPartition], ro: RelativeOffset,
   eof: EndOffset): TopicsConsumer[K,V,KD,VD, MessageAndMetadata[K,V]] = {
    val kc = new KafkaCluster(kafkaParams)
    TopicsConsumer[K,V,KD,VD](kc, topicpartitions, ro, eof)
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kafkaParams: Map[String, String], topicpartitions: Set[TopicAndPartition], ro: RelativeOffset)
  : TopicsConsumer[K,V,KD,VD, MessageAndMetadata[K,V]] = {
    val kc = new KafkaCluster(kafkaParams)
    TopicsConsumer[K,V,KD,VD](kc, topicpartitions, ro, EndOffset(EndOffset.UNLIMITED))
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kafkaParams: Map[String, String], topic: String, ao: AbsoluteOffset,
   eof: EndOffset): TopicsConsumer[K,V,KD,VD, MessageAndMetadata[K,V]] = {
    val kc = new KafkaCluster(kafkaParams)
    val topicpartitions = kc.getPartitions(Set(topic)).right.get
    TopicsConsumer[K,V,KD,VD](kc, topicpartitions, ao, eof)
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kafkaParams: Map[String, String], topic: String, ao: AbsoluteOffset)
  : TopicsConsumer[K,V,KD,VD, MessageAndMetadata[K,V]] = {
    val kc = new KafkaCluster(kafkaParams)
    val topicpartitions = kc.getPartitions(Set(topic)).right.get
    TopicsConsumer[K,V,KD,VD](kc, topicpartitions, ao, EndOffset(EndOffset.UNLIMITED))
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kafkaParams: Map[String, String], topic: String, ro: RelativeOffset,
   eof: EndOffset): TopicsConsumer[K,V,KD,VD, MessageAndMetadata[K,V]] = {
    val kc = new KafkaCluster(kafkaParams)
    val topicpartitions = kc.getPartitions(Set(topic)).right.get
    TopicsConsumer[K,V,KD,VD](kc, topicpartitions, ro, eof)
  }

  def apply[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag]
  (kafkaParams: Map[String, String], topic: String, ro: RelativeOffset)
  : TopicsConsumer[K,V,KD,VD, MessageAndMetadata[K,V]] = {
    val kc = new KafkaCluster(kafkaParams)
    val topicpartitions = kc.getPartitions(Set(topic)).right.get
    TopicsConsumer[K,V,KD,VD](kc, topicpartitions, ro, EndOffset(EndOffset.UNLIMITED))
  }

  def createConsumerThread[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag](tc: TopicsConsumer[K,V,KD,VD,MessageAndMetadata[K, V]])
                             (msgCallback: (MessageAndMetadata[K,V]) => Unit)(implicit system: ActorSystem) = new Thread(new Runnable {
    def run() {
      @tailrec
      def startConsumer(): Unit = {
        Try {
          tc.foreach(msgCallback)
        } match {
          case Success(w) => system.log.warning(s"Consumer thread stopped for ${tc.kafkaPartitions.mkString("[", ", ", "]")}")
          case Failure(e) =>
            system.log.error("Consumer thread for {} crashed. Restarting. e: {}", tc.kafkaPartitions.foreach(_.toString), ExceptionUtils.getStackTrace(e) )
            startConsumer()
        }
      }
      startConsumer()
    }
  })


  case class RelativeOffset(o: Long)
  case class AbsoluteOffset(o: Long)
  case class EndOffset(o: Long) {
    def fromCurrentLatest = o == EndOffset.FROM_CURRENT_LATEST
    def unlimited = o == EndOffset.UNLIMITED
  }
  object EndOffset {
    val UNLIMITED = -2
    val FROM_CURRENT_LATEST = -1
  }

}
// scalastyle:on
