package com.minglim.akkascheduledjobs.scheduledjobsframework.metrics

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.RunningJob
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.RunningJob
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import org.joda.time.LocalDateTime

import scala.concurrent.{ExecutionContext, Future}

object ScheduledJobsMetrics {

  object ScheduleMasterMetrics {
    case class JobRescheduled(key: String) extends ScheduledJobMetric {
      val metric = "job-rescheduled"
    }
  }

  object JobSchedulerMetrics {
    case class JobScheduled(key: String)        extends ScheduledJobMetric {
      val metric = "job-scheduled"
    }
    case class SchedulerAckTimeout(key: String) extends ScheduledJobMetric {
      val metric = "scheduler-ack-timeout"
    }
  }

  object DispatcherMetrics {
    case class JobDispatched(key: String)                            extends ScheduledJobMetric {
      val metric = "job-dispatched"
    }
    case class JobEnqueued(key: String)                              extends ScheduledJobMetric {
      val metric = "job-enqueued"
    }
    case class JobDropped(key: String, reason: String)               extends ScheduledJobMetric {
      val metric        = "job-dropped"
      override val tags = Map("reason" -> reason)
    }
    case class JobRunDuration(runningJob: RunningJob)                extends ScheduledJobMetric {
      val key            = runningJob.key
      val metric         = "job-run-duration"
      override val value = getProcessDuration(runningJob.started)
    }
    case class JobStopped(key: String)                               extends ScheduledJobMetric {
      val metric = "job-stopped"
    }
    case class ConcurrentJobs(key: String, override val value: Long) extends ScheduledJobMetric {
      val metric = "concurrent-jobs"
    }
  }

  object BatchProcessActorMetrics {
    case class BatchesFetched(key: String, override val value: Long, success: Boolean) extends ScheduledJobMetric {
      val metric        = "batches-fetched"
      override val tags = Map("success" -> success.toString)
    }
    case class FetchDuration(key: String)                                              extends ScheduledJobMetric {
      val metric = "fetch-duration"
    }
    case class BatchProcessed(key: String, success: Boolean, retryable: Boolean)       extends ScheduledJobMetric {
      val metric        = "batch-processed"
      override val tags = Map("success" -> success.toString, "retryable" -> retryable.toString)
    }
    case class ProcessDuration(key: String)                                            extends ScheduledJobMetric {
      val metric = "process-duration"
    }
    case class BatchCommitted(key: String, success: Boolean, retryable: Boolean)       extends ScheduledJobMetric {
      val metric        = "batch-committed"
      override val tags = Map("success" -> success.toString, "retryable" -> retryable.toString)
    }
    case class CommitDuration(key: String)                                             extends ScheduledJobMetric {
      val metric = "commit-duration"
    }
    case class ProcessCompleted(key: String)                                           extends ScheduledJobMetric {
      val metric = "process-completed"
    }
    case class ProcessStopped(key: String)                                             extends ScheduledJobMetric {
      val metric = "process-stopped"
    }
    case class StateTimeout(key: String, state: String)                                extends ScheduledJobMetric {
      val metric        = "state-timeout"
      override val tags = Map("state" -> state)
    }
  }

  private def getProcessDuration(started: LocalDateTime): Long = {
    LocalDateTime.now().toDateTime.getMillis - started.toDateTime.getMillis
  }

}

trait ScheduledJobMetric {
  val key: String
  val metric: String
  val value: Long               = 1
  val tags: Map[String, String] = Map.empty
  val SCHEDULEDJOBS_PREFIX      = "scheduledjobs"

  def report()(implicit scheduledJobsContext: ScheduledJobsContext): Unit = reportInternal(metric, value, tags)

  def measureAsync[T](
      f: => Future[T]
  )(implicit scheduledJobsContext: ScheduledJobsContext, ec: ExecutionContext): Future[T] = {
    val start  = System.currentTimeMillis()
    val result = f.map(response => {
      val elapsed = System.currentTimeMillis - start
      reportInternal(metric, elapsed, tags)
      response
    })
    result
  }

  private def reportInternal(metric: String, value: Long, tags: Map[String, String])(implicit
      scheduledJobsContext: ScheduledJobsContext
  ): Unit = {
    val jobMetric     = s"${SCHEDULEDJOBS_PREFIX}.${metric}"
    val metricsPrefix = scheduledJobsContext.metricsContext.metricsPrefix
    val commonTags    = Map("job" -> key)
    if (metricsPrefix.nonEmpty) {
      scheduledJobsContext.metricsReporter.report(s"$metricsPrefix.$jobMetric", value, tags ++ commonTags)
    } else {
      scheduledJobsContext.metricsReporter.report(jobMetric, value, tags ++ commonTags)
    }
  }

}
