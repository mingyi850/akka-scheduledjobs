package com.minglim.akkascheduledjobs.scheduledjobsframework.context

import com.minglim.akkascheduledjobs.scheduledjobsframework.{HadoopReporter, NoOpHadoopReporter}
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.{MetricsReporter, NoOpMetricsReporter}
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.ScheduledJobsNoOpConfigWatcher
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.{ScheduledJobsConfigWatcher, ScheduledJobsNoOpConfigWatcher}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext

object TestScheduledJobsContext {
  val defaultTestMetricsReporter = NoOpMetricsReporter
  val defaultTestHadoopReporter  = NoOpHadoopReporter
  val defaultConfigWatcher       = ScheduledJobsNoOpConfigWatcher
  val testApplicationName        = "test-app"
  def context(implicit
      metricsReporter: MetricsReporter = defaultTestMetricsReporter,
      hadoopReporter: HadoopReporter = defaultTestHadoopReporter,
      configWatcher: ScheduledJobsConfigWatcher = defaultConfigWatcher
  )                              = ScheduledJobsContext(testApplicationName)

}
