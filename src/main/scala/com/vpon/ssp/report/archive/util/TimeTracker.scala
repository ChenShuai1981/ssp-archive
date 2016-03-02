package com.vpon.ssp.report.archive.util

import com.vpon.ssp.report.archive.util.PoolWithBlockingQueue.ObjectPoolFactory

class TimeTracker {

  case class TrackingTag(tag: String, time: Long)

  private[this] var trackEnabled = false
  private[this] var lastTick:Long = 0
  private[this] val defaultSize = 32
  private[this] val tracks = new scala.collection.mutable.ArrayBuffer[TrackingTag](defaultSize)

  def setTrackEnabled(enabled: Boolean): Unit = {
    trackEnabled = enabled
    if (trackEnabled) lastTick = System.nanoTime
  }

  def track(tag: String): Unit = {
    if (trackEnabled) {
      this.synchronized {
        tracks += TrackingTag(tag, System.nanoTime - lastTick)
        lastTick = System.nanoTime
      }
    }
  }

  def reset(): Unit = {
    tracks.clear()
    lastTick = 0
  }

  override def toString: String = {
    var total: Long = 0;
    tracks.map { t =>
      total += t.time
      s"${t.tag}: ${t.time/1e6}"
    }.mkString("\n") + s"\ntotal: ${total/1e6}"
  }
 }

class TrackerPoolFactory extends ObjectPoolFactory[TimeTracker] {
  def create(): TimeTracker = new TimeTracker
}
