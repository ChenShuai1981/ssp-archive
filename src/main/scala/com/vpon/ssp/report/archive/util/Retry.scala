package com.vpon.ssp.report.archive.util

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

import akka.actor.Scheduler

object Retry {

  class NeedRetryException(msg: String) extends Exception(msg)

  /**
   * Retry something for several times until it succeeded.
   * @param scheduler The scheduler
   * @param retries How many times to retry.
   * @param retryInterval The delay between each retry. The unit is millisecond.
   * @param task Something to be retried. If task wants to be retried, the Future must failed with NeedRetryException.
   * @tparam T The result type.
   * @return
   */
  def apply[T](scheduler: Scheduler, retries: Int, retryInterval: FiniteDuration)
              (task: => Future[T])
              (implicit ec: ExecutionContext): Future[T] = {

    val result = Promise[T]

    def retry(task: => Future[T], leftRetries: Int) {
      val newTask = task
      newTask.foreach { v =>
        result.complete(Try(v))
      }

      newTask.onFailure {
        case _: NeedRetryException if leftRetries > 0 =>
          scheduler.scheduleOnce(retryInterval) {
            retry(task, leftRetries - 1)
          }
        case e: Exception =>
          result.failure(e)
      }
    }

    retry(task, retries)
    result.future
  }
}
