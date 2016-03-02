package com.vpon.ssp.report.archive.couchbase

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.document.{Document, JsonDocument}
import com.couchbase.client.java.error.DocumentDoesNotExistException
import com.couchbase.client.java.query._
import com.couchbase.client.java.view.{AsyncSpatialViewResult, AsyncViewResult, SpatialViewQuery, ViewQuery}
import com.couchbase.client.java.{AsyncBucket, PersistTo, ReplicaMode, ReplicateTo}
import rx.{Observable, Observer}

import com.vpon.ssp.report.archive.couchbase.Implicits._

object RxCouchbaseBucket {
  private[RxCouchbaseBucket] implicit class RichCbObs[T](val underlying: Observable[T]) extends AnyVal {
    def toCbGetFuture: Future[T] = {
      val p = Promise[T]()
      underlying.single.subscribe(new Observer[T] {
        def onCompleted(): Unit = {}
        def onNext(t: T): Unit = p success t
        def onError(e: Throwable): Unit = e match {
          //NoSuchElementException is thrown in underlying.single
          case _: NoSuchElementException => p failure new DocumentDoesNotExistException
          case _                         => p failure e
        }
      })
      p.future
    }
  }

//  private[RxCouchbaseBucket] implicit class RichCbObsSeq[T](val underlying: Observable[Seq[T]]) extends AnyVal {
//    def toCbGetFutureSeq: Future[Seq[T]] = {
//      val p = Promise[Seq[T]]()
//      underlying.subscribe(new Observer[Seq[T]] {
//        def onCompleted(): Unit = {}
//        def onNext(t: Seq[T]): Unit = p success t
//        def onError(e: Throwable): Unit = e match {
//          //NoSuchElementException is thrown in underlying.single
//          case _: NoSuchElementException => p failure new DocumentDoesNotExistException
//          case _                         => p failure e
//        }
//      })
//      p.future
//    }
//  }
}

class RxCouchbaseBucket(val asJava: AsyncBucket) {
  import RxCouchbaseBucket._

  @inline def name: String = asJava.name

  def getJ(id: String): Future[JsonDocument] = asJava.get(id).toCbGetFuture

  def get[D <: Document[_]](id: String)(implicit tag: ClassTag[D]): Future[D] =
    asJava.get(id, tag.runtimeClass.asInstanceOf[Class[D]]).toCbGetFuture

//  def getT[T](key: String)(implicit c: Class[_ <: Document[T]]): Future[T] = {
//    asJava.get(key, c).toCbGetFuture.map(_.content)
//  }

  def getBulk[D <: Document[_]](keys: Array[String], inOrder: Boolean = false)(implicit tag: ClassTag[D]): Future[Seq[D]] = {
    val clazz = tag.runtimeClass.asInstanceOf[Class[D]]
    val obs: Observable[String] = Observable.from(keys)
    def f (key: String): Observable[D] = asJava.get(key, clazz)
    val observable: Observable[D] = inOrder match {
      case true => obs.scConcatMap(f)
      case false => obs.scFlatMap(f)
    }
    val javaList = observable.toList
    val scalaList: Observable[List[D]] = javaList.scMap(jl => {
      import scala.collection.JavaConverters._
      jl.asScala.toList
    })
    scalaList.toCbGetFuture
  }

//  def getFromReplica[D <: Document[_]](id: String, tpe: ReplicaMode)(implicit tag: ClassTag[D]): Future[D] =
//    asJava.getFromReplica(id, tpe, tag.runtimeClass.asInstanceOf[Class[D]]).toCbGetFuture
//
//  def getAndLock[D <: Document[_]](id: String, lockTime: Int)(implicit tag: ClassTag[D]): Future[D] =
//    asJava.getAndLock(id, lockTime, tag.runtimeClass.asInstanceOf[Class[D]]).toCbGetFuture
//
//  def getAndTouch[D <: Document[_]](id: String, expiry: Int)(implicit tag: ClassTag[D]): Future[D] =
//    asJava.getAndTouch(id, expiry, tag.runtimeClass.asInstanceOf[Class[D]]).toCbGetFuture

  def insert[D <: Document[_]](document: D): Future[D] = asJava.insert(document).toFuture
//  def insert[D <: Document[_]](document: D, persistTo: PersistTo): Future[D] = asJava.insert(document, persistTo).toFuture
//  def insert[D <: Document[_]](document: D, replicateTo: ReplicateTo): Future[D] = asJava.insert(document, replicateTo).toFuture
//  def insert[D <: Document[_]](document: D, persistTo: PersistTo, replicateTo: ReplicateTo): Future[D] =
//    asJava.insert(document, persistTo, replicateTo).toFuture

  def insertBulk[D <: Document[_]](docs: Seq[D], inOrder: Boolean = false)(implicit tag: ClassTag[D]): Future[Seq[D]] = {
    val obs: Observable[D] = Observable.from(docs)
    def f(doc: D): Observable[D] = asJava.insert(doc)
    val observable: Observable[D] = inOrder match {
      case true => obs.scConcatMap(f)
      case false => obs.scFlatMap(f)
    }
    val javaList: Observable[java.util.List[D]] = observable.toList
    val scalaList: Observable[List[D]] = javaList.scMap(jl => {
      import scala.collection.JavaConverters._
      jl.asScala.toList
    })
    scalaList.toCbGetFuture
  }

  def upsert[D <: Document[_]](document: D): Future[D] = asJava.upsert(document).toFuture
//  def upsert[D <: Document[_]](document: D, persistTo: PersistTo): Future[D] = asJava.upsert(document, persistTo).toFuture
//  def upsert[D <: Document[_]](document: D, replicateTo: ReplicateTo): Future[D] = asJava.upsert(document, replicateTo).toFuture
//  def upsert[D <: Document[_]](document: D, persistTo: PersistTo, replicateTo: ReplicateTo): Future[D] =
//    asJava.upsert(document, persistTo, replicateTo).toFuture

  def upsertBulk[D <: Document[_]](docs: Seq[D], inOrder: Boolean = false): Future[Seq[D]] = {
    val obs: Observable[D] = Observable.from(docs)
    def f(doc: D): Observable[D] = asJava.upsert(doc)
    val observable: Observable[D] = inOrder match {
      case true => obs.scConcatMap(f)
      case false => obs.scFlatMap(f)
    }
    val javaList: Observable[java.util.List[D]] = observable.toList
    val scalaList: Observable[List[D]] = javaList.scMap(jl => {
      import scala.collection.JavaConverters._
      jl.asScala.toList
    })
    scalaList.toCbGetFuture
  }

  def replace[D <: Document[_]](document: D): Future[D] = asJava.replace(document).toFuture
//  def replace[D <: Document[_]](document: D, persistTo: PersistTo): Future[D] = asJava.replace(document, persistTo).toFuture
//  def replace[D <: Document[_]](document: D, replicateTo: ReplicateTo): Future[D] = asJava.replace(document, replicateTo).toFuture
//  def replace[D <: Document[_]](document: D, persistTo: PersistTo, replicateTo: ReplicateTo): Future[D] =
//    asJava.replace(document, persistTo, replicateTo).toFuture

//  def replaceBulk[D <: Document[_]](docs: Seq[D], inOrder: Boolean = false)(implicit tag: ClassTag[D]): Future[Seq[D]] = {
//    val obs: Observable[D] = Observable.from(docs)
//    def f(doc: D): Observable[D] = asJava.replace(doc)
//    val observable: Observable[D] = inOrder match {
//      case true => obs.scConcatMap(f)
//      case false => obs.scFlatMap(f)
//    }
//    val javaList: Observable[java.util.List[D]] = observable.toList
//    val scalaList: Observable[List[D]] = javaList.scMap(jl => {
//      import scala.collection.JavaConverters._
//      jl.asScala.toList
//    })
//    scalaList.toCbGetFuture
//  }

  def remove[D <: Document[_]](document: D): Future[D] = asJava.remove(document).toFuture
//  def remove[D <: Document[_]](document: D, persistTo: PersistTo): Future[D] = asJava.remove(document, persistTo).toFuture
//  def remove[D <: Document[_]](document: D, replicateTo: ReplicateTo): Future[D] = asJava.remove(document, replicateTo).toFuture
//  def remove[D <: Document[_]](document: D, persistTo: PersistTo, replicateTo: ReplicateTo): Future[D] =
//    asJava.remove(document, persistTo, replicateTo).toFuture

//  def remove[D <: Document[_]](id: String)(implicit tag: ClassTag[D]): Future[D] = asJava.remove(id, tag.runtimeClass.asInstanceOf[Class[D]]).toFuture
//  def remove[D <: Document[_]](id: String, persistTo: PersistTo)(implicit tag: ClassTag[D]): Future[D] =
//    asJava.remove(id, persistTo, tag.runtimeClass.asInstanceOf[Class[D]]).toFuture
//  def remove[D <: Document[_]](id: String, replicateTo: ReplicateTo)(implicit tag: ClassTag[D]): Future[D] =
//    asJava.remove(id, replicateTo, tag.runtimeClass.asInstanceOf[Class[D]]).toFuture
//  def remove[D <: Document[_]](id: String, persistTo: PersistTo, replicateTo: ReplicateTo)(implicit tag: ClassTag[D]): Future[D] =
//    asJava.remove(id, persistTo, replicateTo, tag.runtimeClass.asInstanceOf[Class[D]]).toFuture

//  def removeBulk[D <: Document[_]](keys: Array[String], inOrder: Boolean = false)(implicit tag: ClassTag[D]): Future[Seq[D]] = {
//    val clazz = tag.runtimeClass.asInstanceOf[Class[D]]
//    val obs: Observable[String] = Observable.from(keys)
//    def f (key: String): Observable[D] = asJava.remove(key, clazz)
//    val observable: Observable[D] = inOrder match {
//      case true => obs.scConcatMap(f)
//      case false => obs.scFlatMap(f)
//    }
//    val javaList = observable.toList
//    val scalaList: Observable[List[D]] = javaList.scMap(jl => {
//      import scala.collection.JavaConverters._
//      jl.asScala.toList
//    })
//    scalaList.toCbGetFuture
//  }

//  def query(query: ViewQuery): Future[AsyncViewResult] = asJava.query(query).toFuture
//  def query(query: SpatialViewQuery): Future[AsyncSpatialViewResult] = asJava.query(query).toFuture
//  def query(query: N1qlQuery): Future[AsyncN1qlQueryResult] = asJava.query(query).toFuture
//  def query(query: Statement): Future[AsyncN1qlQueryResult] = asJava.query(query).toFuture
//
//  def unlock(id: String, cas: Long): Future[Boolean] = asJava.unlock(id, cas).toFuture.map(_.booleanValue)
//  def unlock[D <: Document[_]](document: D): Future[Boolean] = asJava.unlock(document).toFuture.map(_.booleanValue)
//
//  def touch(id: String, expiry: Int): Future[Boolean] = asJava.touch(id, expiry).toFuture.map(_.booleanValue)
//  def touch[D <: Document[_]](document: D): Future[Boolean] = asJava.touch(document).toFuture.map(_.booleanValue)
//
//  def counter(id: String, delta: Long): Future[Long] = asJava.counter(id, delta).toFuture.map(_.content.longValue)
//  def counter(id: String, delta: Long, initial: Long, expiry: Int = 0): Future[Long] =
//    asJava.counter(id, delta, initial, expiry).toFuture.map(_.content.longValue)
//
//  /** @note the result document has expiry = 0 & content = null
//    * @see https://github.com/couchbase/couchbase-java-client/commit/6f0c7cf2247a3ef99a71ef2edd67f1077e4646e0
//    */
  def append[D <: Document[_]](document: D): Future[D] = asJava.append(document).toFuture
//  /** @note the result document has expiry = 0 & content = null
//    * @see https://github.com/couchbase/couchbase-java-client/commit/6f0c7cf2247a3ef99a71ef2edd67f1077e4646e0
//    */
//  def prepend[D <: Document[_]](document: D): Future[D] = asJava.prepend(document).toFuture
//
//  def bucketManager: Future[AsyncBucketManager] = asJava.bucketManager().toFuture

  def close(): Future[Boolean] = asJava.close().toFuture.map(_.booleanValue)

}
