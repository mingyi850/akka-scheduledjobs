package com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware

import com.typesafe.config.Config

import scala.util.Try

case class ScheduledJobsLeaderAwareSettings (
    zookeeperEnabled: Boolean,
    crossDcLeaderEnabled: Boolean)

object ScheduledJobsLeaderAwareSettings {
  def fromSubConfig(config: Config): ScheduledJobsLeaderAwareSettings = {
    val zookeeperEnabled = Try(config.getBoolean("zookeeper.enabled")).getOrElse(false)
    val crossDcLeaderEnabled = Try(config.getBoolean("cross-dc-leader.enabled")).getOrElse(false)
    ScheduledJobsLeaderAwareSettings(zookeeperEnabled, crossDcLeaderEnabled)
  }

  def default: ScheduledJobsLeaderAwareSettings = {
    ScheduledJobsLeaderAwareSettings(false, false)
  }
}
