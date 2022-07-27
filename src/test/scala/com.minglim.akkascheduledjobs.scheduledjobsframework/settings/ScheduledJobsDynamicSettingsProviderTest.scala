package com.minglim.akkascheduledjobs.scheduledjobsframework.settings

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.{TestBatchProcessorActorTyped, TestSettings}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.{ScheduledJobsDynamicSettingsProvider, TimeoutSettings}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.TimeUnit

class ScheduledJobsDynamicSettingsProviderTest extends AnyWordSpec with Matchers {

  object TestSettings extends ScheduledJobsDynamicSettingsProvider[TestSettings] {
    def key                   = TestBatchProcessorActorTyped.key
    def apply(config: Config) =
      new TestSettings(
        batchInterval = config.getDuration("batch-interval", TimeUnit.MILLISECONDS).milliseconds,
        retries = config.getInt("retries"),
        fetchRetryDelay = config.getDuration("fetch-retry-delay", TimeUnit.MILLISECONDS).milliseconds,
        doProcess = config.getBoolean("do-process"),
        timeoutSettings = Try(TimeoutSettings(config.getConfig("timeout-settings"))).getOrElse(TimeoutSettings.default)
      )
  }
  "getSettings" should {
    "Parse config correctly" in {
      implicit val scheduledJobsContext: ScheduledJobsContext = ScheduledJobsContext.default
      TestSettings.getSettings shouldEqual new TestSettings(5.seconds, 3, 3.seconds, true, TimeoutSettings.default)
    }
  }
}
