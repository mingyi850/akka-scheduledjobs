package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJobsStartupActor.{StartupMessage, StartupScheduledJobs}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.{JobSchedulerActor, ScheduleMasterActor}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor.{ScheduleMasterMessage, StartSchedulers}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.LeaderAware
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}

import scala.concurrent.ExecutionContext

class ScheduledJobsStartupActor(implicit
    leaderAware: LeaderAware,
    scheduledJobsContext: ScheduledJobsContext,
    ec: ExecutionContext
) extends StrictScheduledJobsLogging {

  def Start[BindingsT](
      jobs: List[ScheduledJob[BindingsT]],
      bindings: BindingsT,
      context: ActorContext[StartupMessage]
  ): Behavior[StartupMessage] = {
    val dispatcher     = context.spawn(JobDispatcherActor(jobs, bindings), "dispatcher-actor")
    val scheduleMaster = context.spawn(ScheduleMasterActor(jobs, dispatcher), "scheduleMaster-actor")
    scheduleMaster ! StartSchedulers
    logInfo("Starting scheduled jobs")
    Behaviors.ignore
  }
}

object ScheduledJobsStartupActor {
  sealed trait StartupMessage
  case class StartupScheduledJobs[BindingsT](jobs: List[ScheduledJob[BindingsT]]) extends StartupMessage

  def apply[BindingsT](jobs: List[ScheduledJob[BindingsT]], bindings: BindingsT)(implicit
      scheduledJobsContext: ScheduledJobsContext,
      leaderAware: LeaderAware,
      ec: ExecutionContext
  ): Behavior[StartupMessage] = {
    Behaviors.setup { ctx =>
      new ScheduledJobsStartupActor().Start(jobs, bindings, ctx)
    }
  }
}
