package com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper

import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.MetricsReporter
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC.DC
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.ScheduledJobsConfigWatcher
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.LeaderAware
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.DynamicSettingsListener
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.atomic.AtomicBoolean

class ConsulDCZookeeperLeaderAwareImpl(
                                        dc: DC,
                                        intraDcLeaderAware: LeaderAware,
                                        consulDCLeaderAwareSettings: ConsulDCLeaderAwareSettings,
                                        configWatcher: ScheduledJobsConfigWatcher
)(implicit metricsReporter: MetricsReporter)
    extends LeaderAware
    with StrictLogging
    with DynamicSettingsListener {

  configWatcher.addListener(this)

  val isDCActive = new AtomicBoolean(consulDCLeaderAwareSettings.activeDC == dc)

  override def onSettingsChange(newConfig: Config): Unit = {
    val key = ConsulDCLeaderAwareSettings.configPath
    if (newConfig.hasPath(key)) {
      Try(ConsulDCLeaderAwareSettings(newConfig)) match {
        case Success(updatedConfig) =>
          val isNowActive = updatedConfig.activeDC == dc
          if (isNowActive != isDCActive.get()) {
            isDCActive.set(isNowActive)
            logger.info(
              s"Updated ConsulDCLeaderAware. Active DC: ${updatedConfig.activeDC} DC Active is set to: ${isDCActive.get()}"
            )
          } else {
            logger.info(
              s"No change to DC Activeness: Updated config DC: ${updatedConfig.activeDC}, is Active: ${isDCActive.get()}"
            )
          }
        case Failure(ex)            =>
          logger.error(s"Failed to read config as ConsulDCLeaderAwareSettings: ${newConfig.toString}", ex)
      }
    } else {
      logger.error(
        s"Config doesn't include the key for ConsulDCLeaderAwareSettings key: ${key} config: ${newConfig.toString}"
      )
    }
  }

  override def isLeader(): Boolean = isDCActive.get() && intraDcLeaderAware.isLeader()
}

case class ConsulDCLeaderAwareSettings(activeDC: DC)

object ConsulDCLeaderAwareSettings {
  def configPath: String = "dc-leader-aware"

  def fromSubConfig(config: Config): ConsulDCLeaderAwareSettings = {
    new ConsulDCLeaderAwareSettings(
      activeDC = DC.parse(config.getString("active-dc"))
    )
  }
  def apply(config: Config): ConsulDCLeaderAwareSettings = {
    val conf = config.getConfig(configPath)
    fromSubConfig(conf)
  }
}
