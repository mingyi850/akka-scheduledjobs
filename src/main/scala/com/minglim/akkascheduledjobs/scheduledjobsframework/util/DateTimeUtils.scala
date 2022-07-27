package com.minglim.akkascheduledjobs.scheduledjobsframework.util

import org.joda.time.{DateTime, LocalDateTime, Duration}

import scala.concurrent.duration.{FiniteDuration}

object DateTimeUtils {

  implicit class AugmentedLocalDatetime(localDateTime: LocalDateTime) {
    def +(duration: FiniteDuration): LocalDateTime = {
      localDateTime.plus(Duration.millis(duration.toMillis))
    }

    def -(duration: FiniteDuration): LocalDateTime = {
      localDateTime.minus(Duration.millis(duration.toMillis))
    }
  }

  implicit class AugmentedDatetime(dateTime: DateTime) {
    def +(duration: FiniteDuration): DateTime = {
      dateTime.plus(Duration.millis(duration.toMillis))
    }

    def -(duration: FiniteDuration): DateTime = {
      dateTime.minus(Duration.millis(duration.toMillis))
    }
  }

}
