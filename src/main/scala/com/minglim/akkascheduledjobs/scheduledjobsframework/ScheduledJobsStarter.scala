package com.minglim.akkascheduledjobs.scheduledjobsframework

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJob
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.ScheduledJobsLeaderAware
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper.ZookeeperLeaderAwareSettings
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.{ScheduledJob, ScheduledJobsStartupActor}
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC.DC
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.ScheduledJobsConfigWatcher
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.{LeaderAware, ScheduledJobsLeaderAware}
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.{HadoopReporter, MetricsReporter}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext

object ScheduledJobsStarter {

  def init[BindingsT](appName: String, jobList: List[ScheduledJob[BindingsT]], bindings: BindingsT, leaderAwareOpt: Option[LeaderAware] = None)(implicit
      executionContext: ExecutionContext,
      metricsReporter: MetricsReporter,
      hadoopReporter: HadoopReporter
  ) = {

    val config                        = ConfigFactory.load()
    implicit val scheduledJobsContext = ScheduledJobsContext.fromConfigRoot(config)
    val leaderAware = leaderAwareOpt.getOrElse(ScheduledJobsLeaderAware.fromConfig(config))

    ActorSystem(
      ScheduledJobsStartupActor(jobList, bindings)(scheduledJobsContext, leaderAware, executionContext),
      s"$appName-scheduledjobs-system"
    )
  }
}
