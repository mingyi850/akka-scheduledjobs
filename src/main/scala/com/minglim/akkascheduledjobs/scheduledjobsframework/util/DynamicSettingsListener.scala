package com.minglim.akkascheduledjobs.scheduledjobsframework.util

import com.typesafe.config.Config

trait DynamicSettingsListener {
  def onSettingsChange(config: Config): Unit
}
