package com.minglim.akkascheduledjobs.scheduledjobsframework.settings

import com.typesafe.config.Config

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

trait BatchProcessActorSettings[SettingsT] extends ScheduledJobSettings[SettingsT] {

  /** Interval to wait between each batch */
  val batchInterval: FiniteDuration

  /** How many times to retry fetching and batchProcess operations in case of any failure */
  val retries: Int

  /** How long to wait before attempting fetch again in the event of fetch failure */
  val fetchRetryDelay: FiniteDuration

  /** Should the job run processing function? Used as panic killswitch for a running job */
  val doProcess: Boolean
  val timeoutSettings: TimeoutSettings
}

/** Timeouts for each state of jobProcessing - implement to ensure that actions are retried in a timely manner to keep the job going and move the job into the next state */
case class TimeoutSettings(
    fetchTimeout: FiniteDuration,
    processTimeout: FiniteDuration,
    waitTimeout: FiniteDuration,
    commitTimeout: FiniteDuration,
    finishTimeout: FiniteDuration
)

object TimeoutSettings {
  //default constructors
  def apply(config: Config) = {
    new TimeoutSettings(
      fetchTimeout = config.getDuration("fetch-timeout", TimeUnit.MILLISECONDS).milliseconds,
      processTimeout = config.getDuration("process-timeout", TimeUnit.MILLISECONDS).milliseconds,
      waitTimeout = config.getDuration("wait-timeout", TimeUnit.MILLISECONDS).milliseconds,
      commitTimeout = config.getDuration("commit-timeout", TimeUnit.MILLISECONDS).milliseconds,
      finishTimeout = config.getDuration("finish-timeout", TimeUnit.MILLISECONDS).milliseconds,
    )
  }
  def default    =
    TimeoutSettings(
      fetchTimeout = 100.seconds,
      processTimeout = 100.seconds,
      commitTimeout = 100.seconds,
      waitTimeout = 100.seconds,
      finishTimeout = 100.seconds
    )
  def long       =
    TimeoutSettings(
      fetchTimeout = 600.seconds,
      processTimeout = 600.seconds,
      commitTimeout = 600.seconds,
      waitTimeout = 600.seconds,
      finishTimeout = 600.seconds
    )
  def responsive =
    TimeoutSettings(
      10.seconds,
      processTimeout = 10.seconds,
      commitTimeout = 100.seconds,
      waitTimeout = 10.seconds,
      finishTimeout = 10.seconds
    )
}
