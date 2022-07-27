package com.minglim.akkascheduledjobs.scheduledjobsframework.context

import FrameworkLogLevel.{DEBUG, ERROR, INFO, WARN}
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC.DC
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC
import com.typesafe.scalalogging.{LazyLogging, Logger, StrictLogging}
import org.slf4j.LoggerFactory

trait StrictScheduledJobsLogging extends StrictLogging {
  def logDebug(msg: String)(implicit ctx: ScheduledJobsContext): Unit =
    if (ctx.frameworkLogLevel <= DEBUG) logger.debug(msg) else ()
  def logInfo(msg: String)(implicit ctx: ScheduledJobsContext): Unit  =
    if (ctx.frameworkLogLevel <= INFO) logger.info(msg)
  def logWarn(msg: String)(implicit ctx: ScheduledJobsContext): Unit  =
    if (ctx.frameworkLogLevel <= WARN) logger.warn(msg)
  def logError(msg: String)(implicit ctx: ScheduledJobsContext): Unit =
    if (ctx.frameworkLogLevel <= ERROR) logger.error(msg)
}

object FrameworkLogLevel extends Enumeration {
  type FrameworkLogLevel = Int
  val DEBUG = 1
  val INFO  = 2
  val WARN  = 3
  val ERROR = 4

  def parse(str: String): FrameworkLogLevel = {
    str match {
      case "debug" | "DEBUG" => DEBUG
      case "info" | "INFO"   => INFO
      case "warn" | "WARN"   => WARN
      case "error" | "ERROR" => ERROR
    }
  }
}
