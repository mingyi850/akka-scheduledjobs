package com.minglim.akkascheduledjobs.scheduledjobsframework.metrics

import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.HadoopReporter
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.MetricsReporter
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.RunningJob
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, ScheduledJobsMetricsContext, TestScheduledJobsContext}
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.ScheduledJobsMetrics.{BatchProcessActorMetrics, DispatcherMetrics, JobSchedulerMetrics, ScheduleMasterMetrics}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJobProcessorMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.ScheduledJobsConfigWatcher
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, ScheduledJobsMetricsContext}
import org.joda.time.LocalDateTime
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{times, verify}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar.mock

class ScheduledJobMetricsTest extends AnyWordSpec with Matchers {

  case class ScheduledJobMetricsTestFixture(ctx: ScheduledJobsContext, metricsReporter: MetricsReporter, jobKey: String)

  def withScheduledJobMetricsTestFixture(testCode: ScheduledJobMetricsTestFixture => Any): Any = {
    implicit val mockMetricsReporter = mock[MetricsReporter]
    implicit val hadoopReporter      = mock[HadoopReporter]
    implicit val configWatcher       = mock[ScheduledJobsConfigWatcher]
    val scheduledJobsContext         = ScheduledJobsContext("test-app")
    val jobKey                       = "some-key"
    testCode(ScheduledJobMetricsTestFixture(scheduledJobsContext, mockMetricsReporter, jobKey))
  }

  "ScheduleMasterMetrics" should {
    "Report JobRescheduled" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      ScheduleMasterMetrics.JobRescheduled(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.job-rescheduled", 1, Map("job" -> f.jobKey))
    }
  }
  "JobSchedulerMetrics" should {
    "Report JobScheduled" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      JobSchedulerMetrics.JobScheduled(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.job-scheduled", 1, Map("job" -> f.jobKey))
    }
    "Report SchedulerAckTimeout" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      JobSchedulerMetrics.SchedulerAckTimeout(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.scheduler-ack-timeout", 1, Map("job" -> f.jobKey))
    }
  }
  "DispatcherMetrics" should {
    "Report Job Dispatched" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      DispatcherMetrics.JobDispatched(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.job-dispatched", 1, Map("job" -> f.jobKey))
    }
    "Report Job Enqueued" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      DispatcherMetrics.JobEnqueued(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.job-enqueued", 1, Map("job" -> f.jobKey))
    }
    "Report Job Dropped" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      DispatcherMetrics.JobDropped(f.jobKey, "duplicate").report()
      verify(f.metricsReporter, times(1)).report(
        "scheduledjobs.job-dropped",
        1,
        Map("job" -> f.jobKey, "reason" -> "duplicate")
      )
    }
    "Report Job Run Duration" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      val mockRef      = mock[ActorRef[ScheduledJobProcessorMessage]]

      val runningJob = RunningJob(f.jobKey, "abcds", mockRef, LocalDateTime.now)
      DispatcherMetrics.JobRunDuration(runningJob).report()
      verify(f.metricsReporter, times(1)).report(
        meq("scheduledjobs.job-run-duration"),
        any[Int],
        meq(Map("job" -> f.jobKey))
      )
    }
    "Report Job Stopped" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      DispatcherMetrics.JobStopped(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.job-stopped", 1, Map("job" -> f.jobKey))
    }
    "Report Concurrent running job count" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      DispatcherMetrics.ConcurrentJobs(f.jobKey, 3).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.concurrent-jobs", 3, Map("job" -> f.jobKey))
    }
  }

  "BatchProcessActorMetrics" should {
    "Report Batches Fetched" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      BatchProcessActorMetrics.BatchesFetched(f.jobKey, 10, true).report()
      verify(f.metricsReporter, times(1)).report(
        "scheduledjobs.batches-fetched",
        10,
        Map("job" -> f.jobKey, "success" -> "true")
      )
    }
    "Report Fetch duration" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      implicit val ec  = ExecutionContext.Implicits.global
      def testFut      = Future.successful(())
      BatchProcessActorMetrics.FetchDuration(f.jobKey).measureAsync(testFut)
      eventually {
        verify(f.metricsReporter, times(1)).report(
          meq("scheduledjobs.fetch-duration"),
          any[Long],
          meq(Map("job" -> f.jobKey))
        )
      }
    }
    "Report Batch Processed" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      BatchProcessActorMetrics.BatchProcessed(f.jobKey, true, false).report()
      verify(f.metricsReporter, times(1)).report(
        "scheduledjobs.batch-processed",
        1,
        Map("job" -> f.jobKey, "success" -> "true", "retryable" -> "false")
      )
    }
    "Report Batch process duration" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      implicit val ec  = ExecutionContext.Implicits.global
      def testFut      = Future.successful(())
      BatchProcessActorMetrics.ProcessDuration(f.jobKey).measureAsync(testFut)
      eventually {
        verify(f.metricsReporter, times(1)).report(
          meq("scheduledjobs.process-duration"),
          any[Long],
          meq(Map("job" -> f.jobKey))
        )
      }
    }
    "Report Batch Committed" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      BatchProcessActorMetrics.BatchCommitted(f.jobKey, false, true).report()
      verify(f.metricsReporter, times(1)).report(
        "scheduledjobs.batch-committed",
        1,
        Map("job" -> f.jobKey, "success" -> "false", "retryable" -> "true")
      )
    }
    "Report Commit duration" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      implicit val ec  = ExecutionContext.Implicits.global
      def testFut      = Future.successful(())
      BatchProcessActorMetrics.CommitDuration(f.jobKey).measureAsync(testFut)
      eventually {
        verify(f.metricsReporter, times(1)).report(
          meq("scheduledjobs.commit-duration"),
          any[Long],
          meq(Map("job" -> f.jobKey))
        )
      }
    }
    "Report Process Completed" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      BatchProcessActorMetrics.ProcessCompleted(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.process-completed", 1, Map("job" -> f.jobKey))
    }
    "Report Process Stopped" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      BatchProcessActorMetrics.ProcessStopped(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("scheduledjobs.process-stopped", 1, Map("job" -> f.jobKey))
    }
    "Report State Timeouts" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx
      BatchProcessActorMetrics.StateTimeout(f.jobKey, "WaitingRequest").report()
      verify(f.metricsReporter, times(1)).report(
        "scheduledjobs.state-timeout",
        1,
        Map("job" -> f.jobKey, "state" -> "WaitingRequest")
      )
    }
  }
  "ScheduledJobMetrics" should {
    "Add prefix if supplied in settings" in withScheduledJobMetricsTestFixture { f =>
      implicit val ctx = f.ctx.copy(metricsContext = ScheduledJobsMetricsContext("prefix", true))(
        f.ctx.metricsReporter,
        f.ctx.hadoopReporter,
        f.ctx.configWatcher
      )
      BatchProcessActorMetrics.ProcessStopped(f.jobKey).report()
      verify(f.metricsReporter, times(1)).report("prefix.scheduledjobs.process-stopped", 1, Map("job" -> f.jobKey))
    }
  }
}
