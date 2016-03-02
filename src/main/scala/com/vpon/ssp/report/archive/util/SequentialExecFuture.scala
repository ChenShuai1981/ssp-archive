package com.vpon.ssp.report.archive.util

import scala.collection.generic.CanBuildFrom
import scala.concurrent._
import scala.language.higherKinds

object SequentialExecFuture {

  /**
   * similar to Future.traverse(TraversableOnce[A])(fn: A => Future[B])
   * but instead of execute each future concurrently,
   * it will execute the future one by one
   */
  def traverse[A, B, C[_] <: Iterable[_]]
  (collection: C[A])(fn: A => Future[B])
  (implicit ec: ExecutionContext, cbf: CanBuildFrom[C[A], B, C[B]]): Future[C[B]] = {
    val builder = cbf()
    builder.sizeHint(collection.size)

    collection.foldLeft (Future.successful(builder)) { (previousFuture, next) =>
      for {
        previousResults <- previousFuture
        next <- fn(next.asInstanceOf[A])
      } yield previousResults += next
    } map { _.result() }
  }
}
