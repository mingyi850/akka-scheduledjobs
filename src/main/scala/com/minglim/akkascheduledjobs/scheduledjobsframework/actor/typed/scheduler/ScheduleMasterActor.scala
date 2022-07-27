package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.{ScheduledJob, ScheduledJobsStartupActor}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.DispatcherMessage
import JobSchedulerActor.SchedulerMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor.{Reschedule, ScheduleMasterMessage, SchedulerConfigChanged, SchedulerStopped, StartSchedulers}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.ScheduledJobsMetrics.ScheduleMasterMetrics
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJob
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.LeaderAware
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.ScheduleSettings
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.MD5HashGeneratorService
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

class ScheduleMasterActor[BindingsT](
    ctx: ActorContext[ScheduleMasterMessage],
    jobs: List[ScheduledJob[BindingsT]],
    dispatcherRoutingPool: ActorRef[DispatcherMessage]
)(implicit
    scheduledJobsContext: ScheduledJobsContext,
    leaderAware: LeaderAware,
    ec: ExecutionContext
) extends StrictScheduledJobsLogging {

  val currentSchedules = mutable.HashMap.empty[String, Option[ScheduleSettings]]
  val schedulers       = mutable.HashMap.empty[String, ActorRef[SchedulerMessage]]

  def Start: Behavior[ScheduleMasterMessage] = {
    currentSchedules ++= (getSettingsMap(jobs))
    Behaviors.receive { (ctx, message) =>
      message match {
        case StartSchedulers =>
          ctx.spawn(
            ConfigChangeListenerActor(ctx.self, newConfig => SchedulerConfigChanged(newConfig)),
            "config-listener"
          )
          currentSchedules.foreach {
            case (key, Some(settings)) =>
              logInfo(s"Starting scheduler for job: ${key}")
              val actorRef = spawnScheduler(key, settings)
              schedulers.update(key, actorRef)
            case (key, None)           => logError(s"Failed to start scheduler for ${key}. No Scheduler Settings found")
          }
          // create one scheduler actor for each scheduleSetting
          Running
      }
    }
  }

  def Running: Behavior[ScheduleMasterMessage] = {
    Behaviors.receive { (ctx, message) =>
      message match {
        case Reschedule(updatedSettings)    => {
          logInfo(s"Rescheduling for: ${updatedSettings}")
          updatedSettings.foreach { case (key, Some(updatedSetting)) =>
            ScheduleMasterMetrics.JobRescheduled(key).report()
            currentSchedules.update(key, Some(updatedSetting))
            schedulers.get(key).foreach(scheduler => ctx.stop(scheduler))
          }
          Behaviors.same
        }
        case SchedulerStopped(key)          => {
          logInfo(s"Schedule has stopped for: ${key}")
          currentSchedules.get(key).flatten match {
            case Some(settings) => {
              val newActorRef = spawnScheduler(key, settings)
              schedulers.update(key, newActorRef)
            }
            case None           => logError(s"Could not find settings for $key")
          }
          Behaviors.same
        }
        case SchedulerConfigChanged(config) =>
          onConfigChanged(config)
          Behaviors.same
      }
    }
  }

  def spawnScheduler(key: String, settings: ScheduleSettings): ActorRef[SchedulerMessage] = {
    ctx.spawnAnonymous(
      JobSchedulerActor(
        key,
        settings,
        dispatcherRoutingPool,
        ctx.self,
        MD5HashGeneratorService(),
        Some(settings.scheduleInterval)
      )
    )
  }

  private def getSettingsMap(
      jobs: List[ScheduledJob[BindingsT]],
      configOpt: Option[Config] = None
  ): Map[String, Option[ScheduleSettings]] = {
    val scheduledJobsConfigs =
      configOpt.getOrElse(scheduledJobsContext.configWatcher.currentMergedConfig).getConfig("scheduledjobs")
    jobs
      .map(_.key)
      .map { key =>
        (key -> Try(ScheduleSettings(scheduledJobsConfigs.getConfig(key).getConfig("schedule"))).toOption)
      }
      .toMap
  }

  def onConfigChanged(config: Config): Unit = {
    logInfo("Scheduler detected config change")
    val newSchedules = getSettingsMap(jobs, Some(config))
    ctx.self ! Reschedule(getChangedSettings(newSchedules))
  }

  private def getChangedSettings(
      newScheduleMap: Map[String, Option[ScheduleSettings]]
  ): Map[String, Option[ScheduleSettings]] = {
    currentSchedules.toMap
      .map { case (key, currentSettings) =>
        newScheduleMap.get(key).flatten match {
          case None                                                           => {
            logError(s"No schedule found for job: ${key}")
            (key, None)
          }
          case Some(newSettings) if Some(newSettings).equals(currentSettings) => {
            logInfo(s"No schedule changes for job: ${key}")
            (key, None)
          }
          case Some(newSettings)                                              => {
            logInfo(s"Changed config for ${key}, ${newSettings}")
            (key, Some(newSettings))
          }
        }
      }
      .filter { case (key, value) => value.nonEmpty }
  }
}

object ScheduleMasterActor {
  sealed trait ScheduleMasterMessage
  case object StartSchedulers                                                   extends ScheduleMasterMessage
  case class Reschedule(updatedSettings: Map[String, Option[ScheduleSettings]]) extends ScheduleMasterMessage
  case class SchedulerStopped(key: String)                                      extends ScheduleMasterMessage
  case class SchedulerConfigChanged(config: Config)                             extends ScheduleMasterMessage

  def apply[BindingsT](jobs: List[ScheduledJob[BindingsT]], dispatcherRoutingPool: ActorRef[DispatcherMessage])(implicit
      scheduledJobsContext: ScheduledJobsContext,
      leaderAware: LeaderAware,
      ec: ExecutionContext
  ): Behavior[ScheduleMasterMessage] =
    Behaviors.setup { ctx =>
      new ScheduleMasterActor(ctx, jobs, dispatcherRoutingPool).Start
    }
}
