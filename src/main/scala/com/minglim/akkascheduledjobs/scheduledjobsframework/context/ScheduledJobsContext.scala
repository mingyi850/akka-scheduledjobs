package com.minglim.akkascheduledjobs.scheduledjobsframework.context

import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.NoOpHadoopReporter
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.{MetricsReporter, NoOpMetricsReporter}
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.ScheduledJobsNoOpConfigWatcher
import FrameworkLogLevel.FrameworkLogLevel
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.{ScheduledJobsConfigWatcher, ScheduledJobsConsulConfigWatcher, ScheduledJobsNoOpConfigWatcher}
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.{HadoopReporter, MetricsReporter}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

case class ScheduledJobsContext(
    applicationName: String,
    metricsContext: ScheduledJobsMetricsContext = ScheduledJobsMetricsContext(),
    frameworkLogLevel: FrameworkLogLevel = FrameworkLogLevel.INFO,
    enableHadoopLogging: Boolean = false,
    enableConsul: Boolean = false
)(implicit
    val metricsReporter: MetricsReporter,
    val hadoopReporter: HadoopReporter,
    val configWatcher: ScheduledJobsConfigWatcher
)

case class ScheduledJobsMetricsContext(metricsPrefix: String = "", enableMetrics: Boolean = true)

object ScheduledJobsContext extends LazyLogging {
  def apply(
      config: Config
  )(implicit metricsReporter: MetricsReporter, hadoopReporter: HadoopReporter): ScheduledJobsContext = {
    val applicationName     = Try(config.getString("application-name")).getOrElse("default-app")
    val metricsContext      =
      Try(ScheduledJobsMetricsContext(config.getConfig("metrics-settings"), applicationName))
        .getOrElse(ScheduledJobsMetricsContext(applicationName))
    val enableHadoopLogging = Try(config.getBoolean("hadoop-enabled")).getOrElse(true)
    val enableConsul        = Try(config.getBoolean("consul-enabled")).getOrElse(false)
    val consulWatchedPaths  = Try(config.getStringList("consul-watched-paths").asScala.toList).getOrElse(List.empty)

    ScheduledJobsContext(
      applicationName = applicationName,
      metricsContext = metricsContext,
      frameworkLogLevel =
        Try(FrameworkLogLevel.parse(config.getString("framework-log-level"))).getOrElse(FrameworkLogLevel.INFO),
      enableHadoopLogging = enableHadoopLogging,
      enableConsul = enableConsul
    )(
      getMetricsReporter(metricsContext.enableMetrics),
      getHadoopReporter(enableHadoopLogging),
      getConfigWatcher(consulWatchedPaths, enableConsul)
    )
  }

  def fromConfigRoot(config: Config)(implicit
      metricsReporter: MetricsReporter,
      hadoopReporter: HadoopReporter
  ): ScheduledJobsContext = {
    val context = Try(ScheduledJobsContext.apply(config.getConfig("scheduledjobs.settings")))
      .getOrElse {
        logger.info("Failed to read scheduledjobs settings, falling back to default")
        ScheduledJobsContext.default
      }

    logger.info(
      s"Initialised scheduledJobsContext as ${context} with metricsReporter: ${context.metricsReporter} and hadoopReporter: ${hadoopReporter}"
    )
    context
  }

  def default: ScheduledJobsContext = {
    ScheduledJobsContext(
      applicationName = "default-scheduledjobs-name",
      metricsContext = ScheduledJobsMetricsContext(),
      frameworkLogLevel = FrameworkLogLevel.INFO,
      enableHadoopLogging = false,
      enableConsul = false
    )(NoOpMetricsReporter, NoOpHadoopReporter, ScheduledJobsNoOpConfigWatcher)
  }

  private def getConfigWatcher(consulWatchedPaths: List[String], enabled: Boolean): ScheduledJobsConfigWatcher = {
    if (enabled) {
      if (consulWatchedPaths.isEmpty) logger.warn("Consul is enabled but consulWatchedPaths is empty")
      ScheduledJobsConsulConfigWatcher(consulWatchedPaths)
    } else ScheduledJobsNoOpConfigWatcher
  }

  private def getMetricsReporter(enabled: Boolean)(implicit metricsReporter: MetricsReporter): MetricsReporter = {
    if (enabled) metricsReporter
    else NoOpMetricsReporter
  }

  private def getHadoopReporter(enabled: Boolean)(implicit hadoopReporter: HadoopReporter): HadoopReporter = {
    if (enabled) hadoopReporter
    else NoOpHadoopReporter
  }
}

object ScheduledJobsMetricsContext {
  def apply(config: Config, defaultPrefix: String): ScheduledJobsMetricsContext = {
    ScheduledJobsMetricsContext(
      metricsPrefix = Try(config.getString("prefix")).getOrElse(defaultPrefix),
      enableMetrics = Try(config.getBoolean("enabled")).getOrElse(true)
    )
  }
}
