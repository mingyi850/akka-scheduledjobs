package com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware

import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper.ZookeeperLeaderAwareSettings
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC.DC
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper.{ConsulDCLeaderAwareSettings, ConsulDCZookeeperLeaderAwareImpl, ZookeeperLeaderAwareImpl, ZookeeperLeaderAwareSettings}
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

object ScheduledJobsLeaderAware extends LazyLogging {
  def fromConfig(config: Config)(implicit scheduledJobsContext: ScheduledJobsContext): LeaderAware = {
    val settings = Try(ScheduledJobsLeaderAwareSettings.fromSubConfig(config.getConfig("scheduledjobs.leaderaware")))
      .getOrElse {
        logger.info("Failed to read scheduledjobs coordination settings, falling back to default AlwaysLeader")
        ScheduledJobsLeaderAwareSettings.default
      }
    val intraDCLeaderAware = if (settings.zookeeperEnabled) {
      logger.info("Zookeeper enabled for intraDC Leader election")
      new ZookeeperLeaderAwareImpl(ZookeeperLeaderAwareSettings(config))
    } else {
      logger.debug("Zookeeper not enabled for intraDC Leader election")
      AlwaysLeader
    }
    val crossDCLeaderAware = if (settings.crossDcLeaderEnabled) {
      logger.info("Cross-DC enabled for cross-dc Leader election")
      val consulDCLeaderAwareSettings   = ConsulDCLeaderAwareSettings(config)
      val dc: DC                        = DC.parse(
        Option(System.getenv("DC"))
          .filter(_.nonEmpty)
          .orElse(Option(System.getProperty("DC")))
          .getOrElse{
            logger.warn("Could not find property DC from environment variables. Please ensure variable DC is populated in environment")
            "docker"
          }
          .toLowerCase
      )
        new ConsulDCZookeeperLeaderAwareImpl(
          dc,
          intraDCLeaderAware,
          consulDCLeaderAwareSettings,
          scheduledJobsContext.configWatcher
        )(scheduledJobsContext.metricsReporter)
    } else {
      logger.debug("Cross-DC not enabled for crossDC Leader election")
      intraDCLeaderAware
    }
    crossDCLeaderAware
  }
}
