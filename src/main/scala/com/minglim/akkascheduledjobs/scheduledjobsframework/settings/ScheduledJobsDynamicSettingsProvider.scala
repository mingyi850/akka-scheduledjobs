package com.minglim.akkascheduledjobs.scheduledjobsframework.settings

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJob
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.ScheduledJobsConfigWatcher
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.sslconfig.util.ConfigLoader

/** Inherit this trait and use getSettings to ensure we always pull config from the updated merged config from dynamic settings source */
trait ScheduledJobsDynamicSettingsProvider[SettingsT <: ScheduledJobSettings[SettingsT]] {
  def key: String
  def apply(config: Config): SettingsT
  def getSettings(implicit scheduledJobsContext: ScheduledJobsContext): SettingsT = {
    val configPath = s"scheduledjobs.${key}"
    val config     = scheduledJobsContext.configWatcher.currentMergedConfig.getConfig(configPath)
    apply(config)
  }
}
