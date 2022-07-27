package com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor.ConfigChangeListenerMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor.ConfigChangeListenerMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.DynamicSettingsListener
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

object ScheduledJobsNoOpConfigWatcher extends ScheduledJobsConfigWatcher with LazyLogging {
  val currentMergedConfig: Config                                                                    = ConfigFactory.load()
  override def addListener(listener: DynamicSettingsListener): Unit                                  =
    logger.info(s"Will not add ${listener} as NoOpConfigWatcher is enabled")
  override def removeListener(listener: DynamicSettingsListener): Unit                               =
    logger.info(s"Will not remove ${listener} as NoOpConfigWatcher is enabled")
  override def addListenerActor(name: String, listener: ActorRef[ConfigChangeListenerMessage]): Unit =
    logger.info(s"Will not add ${listener} as NoOpConfigWatcher is enabled")
  override def removeListenerActor(name: String): Unit                                               =
    logger.info(s"Will not remove ${name} as NoOpConfigWatcher is enabled")
}
