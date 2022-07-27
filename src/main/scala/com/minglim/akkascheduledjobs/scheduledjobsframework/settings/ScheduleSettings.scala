package com.minglim.akkascheduledjobs.scheduledjobsframework.settings

import com.minglim.akkascheduledjobs.scheduledjobsframework.model.JobDedupStrategy.{JobDedupStrategy, RunConcurrent}
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.DateTimeUtils._
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.JobDedupStrategy
import com.typesafe.config.Config
import org.joda.time.{LocalDateTime, Seconds}

/** Settings for JobScheduler to decide how to run the job
  * scheduleInterval: Interval between runs for an existing job
  * anchorTime: Optional - Used to determine the start time of the first run of this job. Useful to determine if a job should at a specific time of day/ day of month etc
  */
case class ScheduleSettings(
    scheduleInterval: FiniteDuration,
    anchorTime: Option[AnchorTime] = None,
    jobDedupStrategy: JobDedupStrategy = RunConcurrent
) {
  def getNextStartTime(from: LocalDateTime = LocalDateTime.now(), initialDelay: Option[FiniteDuration] = None) = {
    anchorTime.map(_.getNextAnchor(from)).getOrElse(from + initialDelay.getOrElse(0.seconds))
  }
}

case class AnchorTime(
    dayOfMonth: Option[Int],
    hourOfDay: Option[Int],
    minuteOfHour: Option[Int],
    secondOfMinute: Option[Int]
) {

  def getNextAnchor(from: LocalDateTime = LocalDateTime.now()): LocalDateTime = {
    getTimeToStartInternal(from, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute)
  }

  private def getTimeToStartInternal(
      expected: LocalDateTime,
      dayOfMonth: Option[Int],
      hourOfDay: Option[Int],
      minuteOfHour: Option[Int],
      secondOfMinute: Option[Int]
  ): LocalDateTime = {
    dayOfMonth
      .map { day =>
        if (expected.getDayOfMonth < day) setToAnchor(expected, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute)
        else if (expected.getDayOfMonth > day)
          setToAnchor(expected.plusMonths(1), dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute)
        else getTimeToStartInternal(expected, None, hourOfDay, minuteOfHour, secondOfMinute)
      }
      .orElse {
        hourOfDay
          .map { hour =>
            if (expected.getHourOfDay < hour) setToAnchor(expected, None, hourOfDay, minuteOfHour, secondOfMinute)
            else if (expected.getHourOfDay > hour)
              setToAnchor(expected.plusDays(1), None, hourOfDay, minuteOfHour, secondOfMinute)
            else getTimeToStartInternal(expected, None, None, minuteOfHour, secondOfMinute)
          }
          .orElse {
            minuteOfHour.map { minute =>
              if (expected.getMinuteOfHour < minute) setToAnchor(expected, None, None, minuteOfHour, secondOfMinute)
              else if (expected.getMinuteOfHour > minute)
                setToAnchor(expected.plusHours(1), None, None, minuteOfHour, secondOfMinute)
              else getTimeToStartInternal(expected, None, None, None, secondOfMinute)
            }
          }
          .orElse {
            secondOfMinute.map { second =>
              if (expected.getSecondOfMinute < second) setToAnchor(expected, None, None, None, secondOfMinute)
              else if (expected.getSecondOfMinute > second)
                setToAnchor(expected.plusMinutes(1), None, None, None, secondOfMinute)
              else getTimeToStartInternal(expected, None, None, None, None)
            }
          }
      }
      .getOrElse(expected)
  }

  private def setToAnchor(
      current: LocalDateTime,
      dayOfMonth: Option[Int],
      hourOfDay: Option[Int],
      minuteOfHour: Option[Int],
      secondOfMinute: Option[Int]
  ): LocalDateTime = {
    dayOfMonth
      .map(day => setToAnchor(current.withDayOfMonth(day), None, hourOfDay, minuteOfHour, secondOfMinute))
      .orElse {
        hourOfDay
          .map(hour => setToAnchor(current.withHourOfDay(hour), None, None, minuteOfHour, secondOfMinute))
          .orElse {
            minuteOfHour
              .map(minute => setToAnchor(current.withMinuteOfHour(minute), None, None, None, secondOfMinute))
              .orElse {
                secondOfMinute.map(second => setToAnchor(current.withSecondOfMinute(second), None, None, None, None))
              }
          }

      }
      .getOrElse(current)
  }

}

object ScheduleSettings {
  def apply(config: Config): ScheduleSettings = {
    val scheduleInterval: FiniteDuration = config.getDuration("schedule-interval", MILLISECONDS).milliseconds
    val anchorTime: Option[AnchorTime]   = Try(AnchorTime(config.getConfig("anchor-time"))).toOption
    val dedupStrategy: JobDedupStrategy  =
      Try(JobDedupStrategy.withName(config.getString("job-dedup-strategy"))).getOrElse(RunConcurrent)
    new ScheduleSettings(scheduleInterval, anchorTime, dedupStrategy)
  }
}

object AnchorTime {
  def apply(config: Config): AnchorTime = {
    val dayOfMonth     = Try(config.getInt("day")).toOption
    val hourOfDay      = Try(config.getInt("hour")).toOption
    val minuteOfHour   = Try(config.getInt("minute")).toOption
    val secondOfMinute = Try(config.getInt("second")).toOption
    AnchorTime(dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute)
  }
}
