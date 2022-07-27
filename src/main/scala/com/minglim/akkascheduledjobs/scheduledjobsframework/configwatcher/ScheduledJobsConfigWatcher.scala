package com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor.{ConfigChangeListenerMessage, ConfigChanged}
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.DynamicSettingsListener
import com.typesafe.config.Config
import akka.actor.typed.ActorRef

trait ScheduledJobsConfigWatcher {
  def currentMergedConfig: Config
  def addListener(listener: DynamicSettingsListener): Unit
  def removeListener(listener: DynamicSettingsListener): Unit
  def addListenerActor(name: String, listener: ActorRef[ConfigChangeListenerMessage]): Unit
  def removeListenerActor(name: String): Unit
}
