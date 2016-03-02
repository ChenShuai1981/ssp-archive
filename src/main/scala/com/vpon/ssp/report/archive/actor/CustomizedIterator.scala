package com.vpon.ssp.report.archive.actor

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder

import com.vpon.ssp.report.archive.kafka.consumer.TopicsConsumer

class CustomizedIterator[V, D <: kafka.serializer.Decoder[_]](batchSize: Int, tc: TopicsConsumer[String, V, StringDecoder, D, MessageAndMetadata[String, V]])
  extends scala.AnyRef with java.util.Iterator[Seq[MessageAndMetadata[String, V]]] {

  override def next() : Seq[MessageAndMetadata[String, V]] = {
    var batch = new scala.collection.mutable.ListBuffer[MessageAndMetadata[String, V]]()
    while({
      val nextMessage = tc.getNextIfAvailable()
      nextMessage match {
        case Some(n) => batch.+=(n)
        case None =>
      }
      nextMessage.isDefined && batch.size < batchSize
    }) {
    }
    batch
  }

  override def hasNext() : scala.Boolean = tc.hasNext

  override def remove(): Unit = throw new UnsupportedOperationException
}
