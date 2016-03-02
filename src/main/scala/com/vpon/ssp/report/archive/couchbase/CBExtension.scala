package com.vpon.ssp.report.archive.couchbase

import akka.actor.{ExtensionId, ExtensionIdProvider, ExtendedActorSystem, Extension}
import com.vpon.ssp.report.archive.config.ArchiveConfig


object CBExtension extends ExtensionId[CBExtension] with ExtensionIdProvider {
  def lookup(): CBExtension.type = CBExtension
  def createExtension(system: ExtendedActorSystem): CBExtension = new CBExtension(system)
}

class CBExtension(system: ExtendedActorSystem) extends Extension with ArchiveConfig {

  val buckets = bucketsMap map {
    case (key, bucketInfo) =>
      val bucket = new RxCouchbaseBucket(couchbaseCluster.openBucket(bucketInfo.name, bucketInfo.password).async())
      (key, BucketWithKeyPrefix(bucket, bucketInfo.keyPrefix))
  }

  system.registerOnTermination {
    buckets foreach {
      case (_, bucketWithKeyPrefix) => bucketWithKeyPrefix.bucket.close()
    }
    couchbaseCluster.disconnect()
  }
}
