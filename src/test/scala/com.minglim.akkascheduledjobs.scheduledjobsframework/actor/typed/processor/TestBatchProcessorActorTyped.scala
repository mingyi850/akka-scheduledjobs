package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.{ScheduledJob, ScheduledJobActor, ScheduledJobProcessorMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.DispatcherMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.BatchProcessActorTyped.{BatchResult, RequestWithRetry}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.TestBatchProcessorActorTyped
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.TestBatchProcessorActorTyped.{TestFailureResponse, TestRequest, TestResponse, TestSuccessResponse}
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.{BatchProcessActorSettings, ScheduledJobsDynamicSettingsProvider, TimeoutSettings}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.{ScheduledJob, ScheduledJobActor}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.BatchProcessActorTyped
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.{BatchProcessActorSettings, TimeoutSettings}
import com.typesafe.config.Config

import java.util.concurrent.TimeUnit

case class TestSettings(
    batchInterval: FiniteDuration,
    retries: Int,
    fetchRetryDelay: FiniteDuration,
    doProcess: Boolean,
    timeoutSettings: TimeoutSettings
) extends BatchProcessActorSettings[TestSettings] {
  override def fetchNewSettings(config: Config) = this

}

class TestBatchProcessorDependency() {
  def fetchAllBatches(): Future[List[TestRequest]]                             = Future.successful(List())
  def processBatch(req: TestRequest): Future[TestResponse]                     = Future.successful(TestSuccessResponse(req))
  def batchRecover(req: TestRequest): PartialFunction[Throwable, TestResponse] = { case e: Throwable =>
    TestFailureResponse(req)
  }
  def commitBatch(res: TestResponse): Future[Unit]                             = Future.successful(())
  def handleFetchComplete(res: TestResponse): Unit                             = ()
  def handleBatchComplete(res: TestResponse): Unit                             = ()
  def handleBatchCommitted(res: TestResponse): Unit                            = ()
  def handleProcessStopped(): Unit                                             = ()
  def handleProcessComplete(): Unit                                            = ()
}

class TestBatchProcessorActorTyped(
    testProbe: ActorRef[TestMessage],
    settings: TestSettings,
    dep: TestBatchProcessorDependency
)(implicit ec: ExecutionContext, scheduledJobsContext: ScheduledJobsContext)
    extends BatchProcessActorTyped[TestRequest, TestResponse, TestSettings](settings) {
  override def fetchAllBatches(): Future[List[TestRequest]] = {
    val res = dep.fetchAllBatches()
    testProbe ! FetchingBatch
    res
  }
  override def processBatch(req: TestRequest): Future[TestResponse] = {
    val result = dep.processBatch(req)
    testProbe ! ProcessingBatch(req)
    result
  }
  override def batchRecover(req: TestRequest): PartialFunction[Throwable, TestResponse] = { case e =>
    val result = dep.batchRecover(req)(e)
    testProbe ! BatchRecover(req)
    result
  }
  override def commitBatch(res: TestResponse): Future[Unit] = {
    val result = dep.commitBatch(res)
    testProbe ! CommittingBatch(res)
    result
  }
  override def handleFetchComplete(requests: List[TestRequest]): Unit = {
    testProbe ! ShowInternalList(this.getRemainingRequests.toList)
  }
  override def handleBatchComplete(res: TestResponse): Unit = {
    testProbe ! BatchComplete
  }
  override def handleBatchCommitted(res: TestResponse): Unit = {
    testProbe ! BatchCommited
  }
  override def handleProcessStopped(): Unit = {
    testProbe ! ProcessStopped
  }
  override def handleProcessComplete(): Unit = {
    testProbe ! ProcessCompleted
  }
}

object TestBatchProcessorActorTyped extends ScheduledJob[TestBindings] {

  override def key: String = "test-batch-actor"

  override def create(bindings: TestBindings)(implicit
      scheduledJobsContext: ScheduledJobsContext,
      ec: ExecutionContext
  ): ScheduledJobActor = {
    val settings   = TestSettings(1.second, 3, 1.second, true, TimeoutSettings.default)
    val dependency = new TestBatchProcessorDependency()
    val testProbe  = ActorSystem(Behaviors.receiveMessage[TestMessage](x => Behaviors.same), "test")
    new TestBatchProcessorActorTyped(testProbe.ref, settings, dependency)
  }

  def apply(
      dependency: TestBatchProcessorDependency,
      settings: TestSettings,
      testProbe: ActorRef[TestMessage]
  )(implicit ec: ExecutionContext, sjContext: ScheduledJobsContext) = {
    new TestBatchProcessorActorTyped(testProbe, settings, dependency)
  }
  case class TestRequest(value: Int)
  abstract class TestResponse(success: Boolean, request: TestRequest) extends BatchResult[TestRequest](success, request)
  case class TestSuccessResponse(req: TestRequest)                    extends TestResponse(true, req)
  case class TestFailureResponse(req: TestRequest)                    extends TestResponse(false, req)
}

case class TestBindings()

sealed trait TestMessage
case object FetchingBatch                                                     extends TestMessage
case class ProcessingBatch(req: TestRequest)                                  extends TestMessage
case class CommittingBatch(res: TestResponse)                                 extends TestMessage
case class BatchRecover(req: TestRequest)                                     extends TestMessage
case class ShowInternalList(requestList: List[RequestWithRetry[TestRequest]]) extends TestMessage
case object BatchComplete                                                     extends TestMessage
case object BatchCommited                                                     extends TestMessage
case object ProcessStopped                                                    extends TestMessage
case object ProcessCompleted                                                  extends TestMessage
