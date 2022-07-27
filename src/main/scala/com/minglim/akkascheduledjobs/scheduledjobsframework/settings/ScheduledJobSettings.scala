package com.minglim.akkascheduledjobs.scheduledjobsframework.settings

import com.typesafe.config.Config

trait ScheduledJobSettings[SettingsT] {
  def isChanged(other: SettingsT): Boolean = {
    !this.equals(other)
  }
  def fetchNewSettings(config: Config): SettingsT
}
