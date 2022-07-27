package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.BatchProcessActorTyped.{FetchJobBatches, ProcessorContext, RequestWithRetry}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.{DispatcherMessage, JobCompleted, JobStopped}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.TestBatchProcessorActorTyped.{TestFailureResponse, TestRequest, TestResponse, TestSuccessResponse}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, TestScheduledJobsContext}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.TimeoutSettings
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.{doReturn, spy, times, verify, when}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}
import org.scalatestplus.mockito.MockitoSugar.mock

class BatchProcessorActorTypedTest extends ScalaTestWithActorTestKit with AnyWordSpecLike with Matchers {

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  case class BatchProcessorActorTypedTestFixture(
      dependency: TestBatchProcessorDependency,
      dispatcher: TestProbe[DispatcherMessage],
      settings: TestSettings,
      testProbe: TestProbe[TestMessage]
  )(implicit val scheduledJobsContext: ScheduledJobsContext, val processorContext: ProcessorContext)

  def withBatchProcessorActorTypedTestFixture(testCode: BatchProcessorActorTypedTestFixture => Any): Any = {
    val testProbe                     = TestProbe[TestMessage](MD5Encoder.MD5(Math.random().toString))
    val dependency                    = getMockDependency()
    val dispatcher                    = testKit.createTestProbe[DispatcherMessage]
    val settings                      = TestSettings(100.milliseconds, 3, 1000.milliseconds, true, TimeoutSettings.long)
    implicit val scheduledJobsContext = TestScheduledJobsContext.context
    implicit val processorContext     = ProcessorContext(TestBatchProcessorActorTyped.key, dispatcher.ref)
    testCode(BatchProcessorActorTypedTestFixture(dependency, dispatcher, settings, testProbe))
  }

  private def getMockDependency(): TestBatchProcessorDependency = {
    val dep = mock[TestBatchProcessorDependency]
    when(dep.fetchAllBatches()).thenReturn(Future.successful(List(TestRequest(1), TestRequest(2), TestRequest(3))))
    Range(0, 5).map { x =>
      when(dep.processBatch(TestRequest(x))).thenReturn(Future.successful(TestSuccessResponse(TestRequest(x))))
      val fn: PartialFunction[Throwable, TestResponse] = { case e: Throwable => TestFailureResponse(TestRequest(x)) }
      when(dep.batchRecover(TestRequest(x))).thenReturn(fn)
    }
    when(dep.commitBatch(any[TestSuccessResponse])).thenReturn(Future.successful(()))
    when(dep.commitBatch(any[TestFailureResponse])).thenReturn(Future.successful(()))
    dep
  }

  "Starting" should {

    "Fetch all batches and populate internal requestList" in withBatchProcessorActorTypedTestFixture { f =>
      val actor = testKit.spawn(
        TestBatchProcessorActorTyped(f.dependency, f.settings, f.testProbe.ref)(
          ec,
          f.scheduledJobsContext
        ).dispatch(f.processorContext)
      )
      f.testProbe.expectMessage(FetchingBatch)
      verify(f.dependency).fetchAllBatches()
      f.testProbe.expectMessage(
        ShowInternalList(
          List(
            RequestWithRetry(TestRequest(1), 3),
            RequestWithRetry(TestRequest(2), 3),
            RequestWithRetry(TestRequest(3), 3)
          )
        )
      )
    }

    "Retry fetch if failed after some delay if retries available" in withBatchProcessorActorTypedTestFixture { f =>
      when(f.dependency.fetchAllBatches())
        .thenReturn(Future.failed(new Exception("No reason")), Future.successful(List(TestRequest(1))))
      val actor = testKit.spawn(
        TestBatchProcessorActorTyped(f.dependency, f.settings, f.testProbe.ref)(
          ec,
          f.scheduledJobsContext
        ).dispatch(f.processorContext)
      )
      f.testProbe.expectMessage(FetchingBatch)
      f.testProbe.expectNoMessage(400.milliseconds)
      f.testProbe.expectMessage(FetchingBatch)
      verify(f.dependency, times(2)).fetchAllBatches()
      f.testProbe.expectMessage(ShowInternalList(List(RequestWithRetry(TestRequest(1), 3))))
    }

    "Retry until no more retries before stopping job " in withBatchProcessorActorTypedTestFixture { f =>
      val modifiedSettings = f.settings.copy(retries = 1)
      when(f.dependency.fetchAllBatches()).thenReturn(
        Future.failed(new Exception("No reason")),
        Future.failed(new Exception("No reason again")),
        Future.successful(List(TestRequest(1)))
      )
      val actor            = testKit.spawn(
        TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
          ec,
          f.scheduledJobsContext
        ).dispatch(f.processorContext)
      )
      f.testProbe.expectMessage(FetchingBatch)
      f.testProbe.expectNoMessage(400.milliseconds)
      f.testProbe.expectMessage(FetchingBatch)
      f.testProbe.expectMessage(ProcessStopped)
    }

    "Timeout should attempt to load jobs" in withBatchProcessorActorTypedTestFixture { f =>
      implicit val processorContext = f.processorContext
      val modifiedTimeouts          = TimeoutSettings.long.copy(fetchTimeout = 1.seconds)
      val modifiedSettings          = f.settings.copy(timeoutSettings = modifiedTimeouts)
      val actor                     = testKit.spawn(
        TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
          ec,
          f.scheduledJobsContext
        ).Starting()
      )
      f.testProbe.expectNoMessage(900.millis)
      f.testProbe.expectMessage(FetchingBatch)
      verify(f.dependency).fetchAllBatches()
      f.testProbe.expectMessage(
        ShowInternalList(
          List(
            RequestWithRetry(TestRequest(1), 3),
            RequestWithRetry(TestRequest(2), 3),
            RequestWithRetry(TestRequest(3), 3)
          )
        )
      )
    }
  }

  "WaitingNext, Processing and Committing" should {
    "Process and commit all entries at specified intervals then complete" in withBatchProcessorActorTypedTestFixture {
      f =>
        val modifiedSettings = f.settings.copy(batchInterval = 500.millis)
        val actor            = testKit.spawn(
          TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
            ec,
            f.scheduledJobsContext
          ).dispatch(f.processorContext)
        )
        f.testProbe.expectMessage(FetchingBatch)
        verify(f.dependency).fetchAllBatches()
        f.testProbe.expectMessage(
          ShowInternalList(
            List(
              RequestWithRetry(TestRequest(1), 3),
              RequestWithRetry(TestRequest(2), 3),
              RequestWithRetry(TestRequest(3), 3)
            )
          )
        )
        Range.inclusive(1, 3).foreach { x =>
          f.testProbe.expectMessage(ProcessingBatch(TestRequest(x)))
          f.testProbe.expectMessage(BatchComplete)
          f.testProbe.expectMessage(CommittingBatch(TestSuccessResponse(TestRequest(x))))
          f.testProbe.expectMessage(BatchCommited)
          f.testProbe.expectNoMessage(400.millis)
        }
        f.testProbe.expectMessage(ProcessCompleted)
        f.dispatcher.expectMessage(JobCompleted(TestBatchProcessorActorTyped.key, actor))
        f.dispatcher.expectMessage(JobStopped(TestBatchProcessorActorTyped.key, actor))
    }

    "Dequeue, but not process and commit if doProcess is false" in withBatchProcessorActorTypedTestFixture { f =>
      val modifiedSettings = f.settings.copy(batchInterval = 500.millis, doProcess = false)
      val actor            = testKit.spawn(
        TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
          ec,
          f.scheduledJobsContext
        ).dispatch(f.processorContext)
      )
      f.testProbe.expectMessage(FetchingBatch)
      verify(f.dependency).fetchAllBatches()
      f.testProbe.expectMessage(
        ShowInternalList(
          List(
            RequestWithRetry(TestRequest(1), 3),
            RequestWithRetry(TestRequest(2), 3),
            RequestWithRetry(TestRequest(3), 3)
          )
        )
      )
      Range.inclusive(1, 3).foreach { x =>
        f.testProbe.expectNoMessage(400.millis)
      }
      f.testProbe.expectMessage(ProcessCompleted)
    }

    "Re-enqueue failed requests for number of retries" in withBatchProcessorActorTypedTestFixture { f =>
      val modifiedSettings = f.settings.copy(batchInterval = 500.millis, retries = 2)
      when(f.dependency.processBatch(TestRequest(1))).thenReturn(Future.successful(TestFailureResponse(TestRequest(1))))
      when(f.dependency.fetchAllBatches()).thenReturn(Future.successful(List(TestRequest(1), TestRequest(2))))
      val actor            = testKit.spawn(
        TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
          ec,
          f.scheduledJobsContext
        ).dispatch(f.processorContext)
      )
      f.testProbe.expectMessage(FetchingBatch)
      verify(f.dependency).fetchAllBatches()
      f.testProbe.expectMessage(
        ShowInternalList(List(RequestWithRetry(TestRequest(1), 2), RequestWithRetry(TestRequest(2), 2)))
      )

      //failure #1
      f.testProbe.expectMessage(ProcessingBatch(TestRequest(1)))
      f.testProbe.expectMessage(BatchComplete)
      f.testProbe.expectNoMessage(400.millis)
      //success #2
      f.testProbe.expectMessage(ProcessingBatch(TestRequest(2)))
      f.testProbe.expectMessage(BatchComplete)
      f.testProbe.expectMessage(CommittingBatch(TestSuccessResponse(TestRequest(2))))
      f.testProbe.expectMessage(BatchCommited)
      f.testProbe.expectNoMessage(400.millis)
      //failure #2
      f.testProbe.expectMessage(ProcessingBatch(TestRequest(1)))
      f.testProbe.expectMessage(BatchComplete)
      f.testProbe.expectNoMessage(400.millis)
      //failure #3 Failure should be committed
      f.testProbe.expectMessage(ProcessingBatch(TestRequest(1)))
      f.testProbe.expectMessage(BatchComplete)
      f.testProbe.expectMessage(CommittingBatch(TestFailureResponse(TestRequest(1))))
      f.testProbe.expectMessage(BatchCommited)
      //finish
      f.testProbe.expectMessage(ProcessCompleted)
    }

    "Recover and Re-enqueue failed requests with exception for number of retries" in withBatchProcessorActorTypedTestFixture {
      f =>
        val modifiedSettings = f.settings.copy(batchInterval = 500.millis, retries = 1)
        when(f.dependency.processBatch(TestRequest(1)))
          .thenReturn(Future.failed(new RuntimeException("Something happened")))
        when(f.dependency.fetchAllBatches()).thenReturn(Future.successful(List(TestRequest(1), TestRequest(2))))
        val actor            = testKit.spawn(
          TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
            ec,
            f.scheduledJobsContext
          ).dispatch(f.processorContext)
        )
        f.testProbe.expectMessage(FetchingBatch)
        verify(f.dependency).fetchAllBatches()
        f.testProbe.expectMessage(
          ShowInternalList(List(RequestWithRetry(TestRequest(1), 1), RequestWithRetry(TestRequest(2), 1)))
        )

        //failure #1
        f.testProbe.expectMessage(ProcessingBatch(TestRequest(1)))
        f.testProbe.expectMessage(BatchRecover(TestRequest(1)))
        f.testProbe.expectMessage(BatchComplete)
        f.testProbe.expectNoMessage(400.millis)
        //success #2
        f.testProbe.expectMessage(ProcessingBatch(TestRequest(2)))
        f.testProbe.expectMessage(BatchComplete)
        f.testProbe.expectMessage(CommittingBatch(TestSuccessResponse(TestRequest(2))))
        f.testProbe.expectMessage(BatchCommited)
        f.testProbe.expectNoMessage(400.millis)
        //failure #3 Failure should be committed
        f.testProbe.expectMessage(ProcessingBatch(TestRequest(1)))
        f.testProbe.expectMessage(BatchRecover(TestRequest(1)))
        f.testProbe.expectMessage(BatchComplete)
        f.testProbe.expectMessage(CommittingBatch(TestFailureResponse(TestRequest(1))))
        f.testProbe.expectMessage(BatchCommited)
        //finish
        f.testProbe.expectMessage(ProcessCompleted)
    }

    "Retry if Commit fails" in withBatchProcessorActorTypedTestFixture { f =>
      val modifiedSettings = f.settings.copy(batchInterval = 500.millis, retries = 2)
      when(f.dependency.commitBatch(TestSuccessResponse(TestRequest(1))))
        .thenReturn(Future.failed(new RuntimeException("Something happened")))
      when(f.dependency.fetchAllBatches()).thenReturn(Future.successful(List(TestRequest(1))))
      val actor            = testKit.spawn(
        TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
          ec,
          f.scheduledJobsContext
        ).dispatch(f.processorContext)
      )
      f.testProbe.expectMessage(FetchingBatch)
      verify(f.dependency).fetchAllBatches()
      f.testProbe.expectMessage(ShowInternalList(List(RequestWithRetry(TestRequest(1), 2))))

      f.testProbe.expectMessage(ProcessingBatch(TestRequest(1)))
      f.testProbe.expectMessage(BatchComplete)
      f.testProbe.expectMessage(CommittingBatch(TestSuccessResponse(TestRequest(1))))
      f.testProbe.expectMessage(BatchCommited)
      f.testProbe.expectMessage(CommittingBatch(TestSuccessResponse(TestRequest(1))))
      f.testProbe.expectMessage(BatchCommited)
      f.testProbe.expectMessage(CommittingBatch(TestSuccessResponse(TestRequest(1))))
      f.testProbe.expectMessage(BatchCommited)
      f.testProbe.expectNoMessage(400.millis)

      //finish
      f.testProbe.expectMessage(ProcessCompleted)
    }

    "Timeout in Waiting should attempt to get Next job" in withBatchProcessorActorTypedTestFixture { f =>
      implicit val processorContext = f.processorContext
      val modifiedTimeouts          = TimeoutSettings.long.copy(waitTimeout = 1.seconds)
      val modifiedSettings          = f.settings.copy(timeoutSettings = modifiedTimeouts)
      val actor                     = testKit.spawn(
        TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
          ec,
          f.scheduledJobsContext
        ).WaitingRequest()
      )
      f.testProbe.expectNoMessage(900.millis)
      f.testProbe.expectMessage(ProcessCompleted) // No more jobs loaded
    }

    "Timeout in Processing while waiting for result should drop job and get Next job" in withBatchProcessorActorTypedTestFixture {
      f =>
        implicit val processorContext = f.processorContext
        val modifiedTimeouts          = TimeoutSettings.long.copy(processTimeout = 1.seconds)
        val modifiedSettings          = f.settings.copy(timeoutSettings = modifiedTimeouts)
        val actor                     = testKit.spawn(
          TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
            ec,
            f.scheduledJobsContext
          ).Processing()
        )
        f.testProbe.expectNoMessage(900.millis)
        f.testProbe.expectMessage(ProcessCompleted) // No more jobs loaded
    }

    "Timeout in Committing while waiting for commit should drop job and get Next job" in withBatchProcessorActorTypedTestFixture {
      f =>
        implicit val processorContext = f.processorContext
        val modifiedTimeouts          = TimeoutSettings.long.copy(commitTimeout = 1.seconds)
        val modifiedSettings          = f.settings.copy(timeoutSettings = modifiedTimeouts)
        val actor                     = testKit.spawn(
          TestBatchProcessorActorTyped(f.dependency, modifiedSettings, f.testProbe.ref)(
            ec,
            f.scheduledJobsContext
          ).Committing()
        )
        f.testProbe.expectNoMessage(900.millis)
        f.testProbe.expectMessage(ProcessCompleted) // No more jobs loaded
    }

  }
}
