package com.vpon.ssp.report.archive.config

import java.util.concurrent.TimeUnit

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.collection.JavaConversions._

import spray.json._
import com.couchbase.client.java.CouchbaseCluster
import com.typesafe.config.ConfigFactory

import com.vpon.ssp.report.archive.couchbase.BucketInfo


trait ArchiveConfig {

  val config = ConfigFactory.load()

  lazy val clusterGroup = config.getString("ssp-archive.cluster-group")
  lazy val clusterRampupTime = config.getDuration("ssp-archive.cluster-rampup-time", TimeUnit.SECONDS).seconds

  lazy val consumeBatchSize = config.getInt("ssp-archive.consume-batch-size")

  /**
   * Retries
   */
  lazy val sendMaxRetries = config.getInt("ssp-archive.retries.send.max")
  lazy val sendRetryInterval = config.getDuration("ssp-archive.retries.send.interval", MILLISECONDS).milliseconds

  lazy val couchbaseMaxRetries = config.getInt("ssp-archive.retries.couchbase.max")
  lazy val couchbaseRetryInterval = config.getDuration("ssp-archive.retries.couchbase.interval", MILLISECONDS).milliseconds

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

  /**
   * AWS S3
   */
  lazy val s3RegionName = config.getString("aws.region-name")
  lazy val s3BucketName = config.getString("aws.s3.bucket-name")
  lazy val s3CompressionType = config.getString("aws.s3.compression-type")
  lazy val s3NeedEncrypt = config.getBoolean("aws.s3.need-encrypt")


  def config2Json(): JsValue = {
    JsObject(
      "ssp-archive" -> JsObject(
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
      ),
      "aws" -> JsObject(
        "region-name" -> JsString(s3RegionName),
        "s3" -> JsObject(
          "bucket-name" -> JsString(s3BucketName),
          "compression-type" -> JsString(s3CompressionType),
          "need-encrypt" -> JsBoolean(s3NeedEncrypt)
        )
      )
    )
  }

}
