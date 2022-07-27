package com.minglim.akkascheduledjobs.scheduledjobsframework.settings

import com.minglim.akkascheduledjobs.scheduledjobsframework.model.JobDedupStrategy.Enqueue
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.{AnchorTime, ScheduleSettings}
import com.typesafe.config.ConfigFactory
import org.joda.time.LocalDateTime
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar.mock

class ScheduledSettingsTest extends AnyWordSpec with Matchers {

  "ScheduleSettings" should {
    "Use from as next runtime if AnchorTime is None and initialDelay is None" in {
      val fromTime = LocalDateTime.parse("2022-01-01T12:03:00.000")
      ScheduleSettings(5.seconds, None).getNextStartTime(fromTime) shouldEqual fromTime
    }

    "Use from + initialDelay as next runtime if AnchorTime is None and initialDelay contains value" in {
      val fromTime     = LocalDateTime.parse("2022-01-01T12:03:00.000")
      val initialDelay = 10.seconds
      val expectedTime = LocalDateTime.parse("2022-01-01T12:03:10.000")
      ScheduleSettings(5.seconds, None).getNextStartTime(fromTime, Some(initialDelay)) shouldEqual expectedTime
    }

    "Use AnchorTime if provided" in {
      val fromTime           = LocalDateTime.parse("2022-01-01T12:03:00.000")
      val supposedAnchorTime = LocalDateTime.parse("2033-01-01T12:03:22.000")
      val anchorTime         = mock[AnchorTime]
      when(anchorTime.getNextAnchor(fromTime)).thenReturn(supposedAnchorTime)
      ScheduleSettings(5.seconds, Some(anchorTime)).getNextStartTime(fromTime) shouldEqual supposedAnchorTime
    }

    "Parse config correctly" in {
      val testConfig =
        """
          |schedule-interval = 5.minutes
          |anchor-time {
          | hour = 5
          | minute = 30
          |}
          |job-dedup-strategy = enqueue
          |""".stripMargin
      val config     = ConfigFactory.parseString(testConfig)
      ScheduleSettings(config) shouldEqual ScheduleSettings(
        5.minutes,
        Some(AnchorTime(None, Some(5), Some(30), None)),
        Enqueue
      )
    }
  }
  "AnchorTime" should {
    "Find next anchor time correctly" in {
      val fromTime   = LocalDateTime.parse("2022-01-01T12:03:00.000")
      val anchorTime =
        AnchorTime(dayOfMonth = Some(20), hourOfDay = Some(13), minuteOfHour = None, secondOfMinute = Some(33))
      anchorTime.getNextAnchor(fromTime) shouldEqual LocalDateTime.parse("2022-01-20T13:03:33.000")

      val anchorTime2 = AnchorTime(dayOfMonth = None, hourOfDay = None, minuteOfHour = None, secondOfMinute = Some(33))
      anchorTime2.getNextAnchor(fromTime) shouldEqual LocalDateTime.parse("2022-01-01T12:03:33.000")
    }
  }
}
