package com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher

import akka.actor.typed.ActorRef
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor.{ConfigChangeListenerMessage, ConfigChanged}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor.ConfigChangeListenerMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.NoOpMetricsReporter
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.DynamicSettingsListener
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.util.Try

case class ScheduledJobsConsulConfigWatcher(consulWatchedPaths: List[String])
    extends ScheduledJobsConfigWatcher
    with LazyLogging {

  /** Specify the list of consul paths in scheduledjobs.consul-watched-paths to watch in order to increasing override priority */
  private val listeners      = mutable.Set[DynamicSettingsListener]()
  private val listenerActors = mutable.HashMap[String, ActorRef[ConfigChangeListenerMessage]]()
  val watcher                = ConsulWatcher
    .builder(ConfigFactory.load())
    .withMetricReporter(NoOpMetricsReporter)
    .build()

  logger.info(s"Watching paths: ${consulWatchedPaths}")
  val watchedConfig = watcher.watchKv(
    consulWatchedPaths
  )

  var currentMergedConfig: Config = Try(getMergedConfig(Option(watchedConfig.get))).getOrElse(ConfigFactory.load())

  // Initiate the watcher

  watchedConfig.map { updatedConfig =>
    logger.debug(s"Config from Consul: ${updatedConfig}")
    val mergedConfig = getMergedConfig(Some(updatedConfig))
    currentMergedConfig = mergedConfig
    logger.debug(s"New Merged Config: ${mergedConfig}")
    listeners.foreach(_.onSettingsChange(mergedConfig))
    listenerActors.values.foreach(actor => actor ! ConfigChanged(mergedConfig))
  }

  private def getMergedConfig(consulConfigOpt: Option[Config]): Config = {
    consulConfigOpt
      .map(conf => conf.withFallback(ConfigFactory.load()))
      .getOrElse(ConfigFactory.load())
  }

  override def addListener(listener: DynamicSettingsListener) = {
    logger.debug(s"ConsulConfigWatcher: adding listener: ${listener}")
    listeners += listener
  }

  override def removeListener(listener: DynamicSettingsListener) = {
    logger.debug(s"ConsulConfigWatcher: removing listener: ${listener}")
    listeners -= listener
  }

  override def addListenerActor(name: String, listener: ActorRef[ConfigChangeListenerMessage]) = {
    logger.debug(s"ConsulConfigWatcher: adding listenerActor with name: ${name},  ${listener}")
    listenerActors.update(name, listener)
  }

  override def removeListenerActor(name: String) = {
    logger.debug(s"ConsulConfigWatcher: removing listenerActor with name: ${name}")
    listenerActors.remove(name)
  }
}
