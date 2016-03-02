package com.vpon.ssp.report.dedup.config

import java.util.concurrent.TimeUnit

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.collection.JavaConversions._

import spray.json.{JsNumber, JsString, JsObject, JsValue}
import com.couchbase.client.java.CouchbaseCluster
import com.typesafe.config.ConfigFactory

import com.vpon.ssp.report.dedup.couchbase.BucketInfo


trait DedupConfig {

  val config = ConfigFactory.load()

  lazy val clusterGroup = config.getString("ssp-kafka-s3.cluster-group")
  lazy val clusterRampupTime = config.getDuration("ssp-kafka-s3.cluster-rampup-time", TimeUnit.SECONDS).seconds

  lazy val consumeBatchSize = config.getInt("ssp-kafka-s3.consume-batch-size")

  /**
   * Retries
   */
  lazy val couchbaseMaxRetries = config.getInt("ssp-kafka-s3.retries.couchbase.max")
  lazy val couchbaseRetryInterval = config.getDuration("ssp-kafka-s3.retries.couchbase.interval", MILLISECONDS).milliseconds

  /**
   * Kafka
   */
  lazy val sourceTopic = config.getString("kafka.consumer.topic")
  lazy val sourceBrokers = config.getString("kafka.consumer.brokers")

  /**
   * Couchbase
   */
  lazy val couchbaseConnectionString = config.getString("couchbase.connection-string")
  lazy val couchbaseCluster = CouchbaseCluster.fromConnectionString(couchbaseConnectionString)
  lazy val bucketsMap = config.getObject("couchbase.buckets").toMap map {
    case (key, value) =>
      val bucketConfig = value.asInstanceOf[com.typesafe.config.ConfigObject].toConfig
      val name = bucketConfig.getString("name")
      val password = bucketConfig.getString("password")
      val keyPrefix = bucketConfig.getString("key-prefix")
      (key, BucketInfo(name, password, keyPrefix))
  }

  def config2Json(): JsValue = {
    JsObject(
      "ssp-kafka-s3" -> JsObject(
        "cluster-group" -> JsString(clusterGroup),
        "cluster-rampup-time" -> JsString(clusterRampupTime.toSeconds + "s"),
        "consume-batch-size" -> JsNumber(consumeBatchSize),
        "retries" -> JsObject(
          "couchbase" -> JsObject(
            "max" -> JsNumber(couchbaseMaxRetries),
            "interval" -> JsString(couchbaseRetryInterval.toMillis + "ms")
          )
        )
      ),
      "couchbase" -> JsObject(
        "connection-string" -> JsString(couchbaseConnectionString),
        "buckets" -> JsObject(
          bucketsMap.map(m =>
            m._1 -> JsObject(
              "name" -> JsString(m._2.name),
              "password" -> JsString(m._2.password),
              "key-prefix" -> JsString(m._2.keyPrefix)
            )
          )
        )
      ),
      "kafka" -> JsObject(
        "consumer" -> JsObject(
          "topic" -> JsString(sourceTopic),
          "brokers" -> JsString(sourceBrokers)
        )
      )
    )
  }

}
