// scalastyle:off
package com.vpon.ssp.report.archive.kafka.producer

import java.util.Properties
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.producer._
import scala.concurrent.duration._


import scala.language.implicitConversions


class KafkaProducerCallbackFactory[V](
  errorcallback:(RecordMetadata, V, Exception) => Unit = (a:RecordMetadata,b:V, c:Exception) => (),
  resultcallback:(RecordMetadata, V, Option[Exception]) => Unit = (a:RecordMetadata,b:V,c:Option[Exception]) => ()) {

  type ErrorCallback = (RecordMetadata, V, Exception) => Unit
  type ResultCallback = (RecordMetadata, V, Option[Exception]) => Unit

  @transient private[this] val produceCounter: AtomicInteger = new AtomicInteger(0)

  def apply(msg: V) = {
    produceCounter.getAndIncrement
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          errorcallback(metadata, msg, exception)
        }
        resultcallback(metadata, msg, Option(exception))

        produceCounter.decrementAndGet()
      }
    }
  }

  def awaitAllMessagesSent(timeout: FiniteDuration, interval: FiniteDuration = 50.milliseconds): Boolean = {
    val infinite: Boolean = timeout.toMicros < 0
    val deadline = timeout.fromNow

    while(infinite || deadline.hasTimeLeft()) {
      if(produceCounter.get() == 0) return true
      Thread.sleep(interval.toMillis)
    }
    return false
  }

}

class KafkaProducerWrapper[K,V](private val producerprops: Properties) {
  @transient private[this] val client = new KafkaProducer[K,V](producerprops)

  def send(key: K, msg: V, topic: String, partition: Int, callback: Callback): Future[RecordMetadata] = {
    client.send(new ProducerRecord[K,V](topic, partition, key, msg), callback)
  }
  def send(key:K, msg: V, topic: String): Future[RecordMetadata] = {
    send(key, msg, topic, null.asInstanceOf[Int], null)
  }
  def send(msg: V, topic: String, partition: Int, callback: Callback): Future[RecordMetadata] = {
    send(null.asInstanceOf[K],msg, topic, partition, callback)
  }
  def send(msg: V, topic: String, partition: Int): Future[RecordMetadata] = {
    send(null.asInstanceOf[K],msg, topic, partition, null)
  }
  def send(msg: V, topic: String): Future[RecordMetadata] = {
    send(null.asInstanceOf[K],msg, topic, null.asInstanceOf[Int], null)
  }
  def send(msg: V, topic: String, partition: Int, callbackfactory: KafkaProducerCallbackFactory[V]): Future[RecordMetadata] = {
    send(null.asInstanceOf[K],msg, topic, partition, callbackfactory(msg))
  }
  def send(msg: V, topic: String, callbackfactory: KafkaProducerCallbackFactory[V]): Future[RecordMetadata] = {
    send(null.asInstanceOf[K],msg, topic, null.asInstanceOf[Int], callbackfactory(msg))
  }

}


object KafkaProducerSingleton {

  @transient private[this] var strClient: KafkaProducerWrapper[String,String] = null
  @transient private[this] var byteClient: KafkaProducerWrapper[Array[Byte],Array[Byte]] = null


  def getStringClient(implicit props: Properties) = synchronized { if (strClient == null) {
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    strClient = new KafkaProducerWrapper[String,String](props)
  }
    strClient
  }
  def getByteClient(implicit props: Properties) = synchronized { if (byteClient == null) {
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    byteClient = new KafkaProducerWrapper[Array[Byte],Array[Byte]](props)
  }
    byteClient
  }

}
// scalastyle:on
