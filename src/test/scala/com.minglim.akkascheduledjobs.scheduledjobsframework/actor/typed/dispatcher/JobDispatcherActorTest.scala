package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJobProcessorMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.{Dispatch, DispatcherMessage, JobStopped}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.{TestBatchProcessorActorTyped, TestBindings}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.TestBatchProcessorActorTyped
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.JobSchedulerActor.{JobAlreadyReceived, JobDropped, JobEnqueued, JobReceived, SchedulerMessage, UnrecognizedJob}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.TestScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJobProcessorMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.JobDedupStrategy.{Drop, Enqueue, RunConcurrent}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JobDispatcherActorTest extends AnyWordSpec with Matchers {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  case class JobDispatcherActorTestFixture(
      testKit: BehaviorTestKit[DispatcherMessage],
      schedulerMock: TestInbox[SchedulerMessage]
  )(implicit val scheduledJobsContext: ScheduledJobsContext)

  def withJobDispatcherActorTestFixture(testCode: JobDispatcherActorTestFixture => Any): Any = {
    implicit val scheduledJobsContext = TestScheduledJobsContext.context
    val bindings                      = TestBindings()
    val jobList                       = List(TestBatchProcessorActorTyped)
    val testKit                       = BehaviorTestKit(JobDispatcherActor(jobList, bindings))
    val schedulerMock                 = TestInbox[SchedulerMessage]()
    testCode(JobDispatcherActorTestFixture(testKit, schedulerMock))
  }

  "Dispatch" should {
    "Create a new instance of specified job if key available and job not already running" in withJobDispatcherActorTestFixture {
      f =>
        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash", f.schedulerMock.ref, RunConcurrent))
        f.testKit
          .expectEffectType[Spawned[ScheduledJobProcessorMessage]]
          .childName shouldEqual s"${TestBatchProcessorActorTyped.key}-hash"
        f.schedulerMock.expectMessage(JobReceived)
    }

    "Run jobs concurrently, but not create a new instance of specified job if job already running (based on hash) for RunConcurrent mode" in withJobDispatcherActorTestFixture {
      f =>
        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash", f.schedulerMock.ref, RunConcurrent))
        f.testKit
          .expectEffectType[Spawned[ScheduledJobProcessorMessage]]
          .childName shouldEqual s"${TestBatchProcessorActorTyped.key}-hash"
        f.schedulerMock.expectMessage(JobReceived)

        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash2", f.schedulerMock.ref, RunConcurrent))
        f.testKit
          .expectEffectType[Spawned[ScheduledJobProcessorMessage]]
          .childName shouldEqual s"${TestBatchProcessorActorTyped.key}-hash2"
        f.schedulerMock.expectMessage(JobReceived)

        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash", f.schedulerMock.ref, RunConcurrent))
        f.testKit.expectEffectType[NoEffects]
        f.schedulerMock.expectMessage(JobAlreadyReceived)
    }

    "Not create a new instance of specified job if job already running (based on key) for Drop mode" in withJobDispatcherActorTestFixture {
      f =>
        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash", f.schedulerMock.ref, Drop))
        f.testKit
          .expectEffectType[Spawned[ScheduledJobProcessorMessage]]
          .childName shouldEqual s"${TestBatchProcessorActorTyped.key}-hash"
        f.schedulerMock.expectMessage(JobReceived)

        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash2", f.schedulerMock.ref, Drop))
        f.testKit
          .expectEffectType[NoEffects]
        f.schedulerMock.expectMessage(JobDropped)
    }

    "Enqueue new instance of specified job if job already running (based on key) for Enqueue mode" in withJobDispatcherActorTestFixture {
      f =>
        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash", f.schedulerMock.ref, Enqueue))
        f.testKit
          .expectEffectType[Spawned[ScheduledJobProcessorMessage]]
          .childName shouldEqual s"${TestBatchProcessorActorTyped.key}-hash"
        f.schedulerMock.expectMessage(JobReceived)

        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash", f.schedulerMock.ref, Enqueue))
        f.testKit
          .expectEffectType[NoEffects]
        f.schedulerMock.expectMessage(JobAlreadyReceived)

        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash2", f.schedulerMock.ref, Enqueue))
        f.testKit
          .expectEffectType[NoEffects]
        f.schedulerMock.expectMessage(JobEnqueued)

        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash2", f.schedulerMock.ref, Enqueue))
        f.testKit
          .expectEffectType[NoEffects]
        f.schedulerMock.expectMessage(JobAlreadyReceived)
    }

    "Dispatch and dequeue instance of specified job if job already running (based on key) for Enqueue mode" in withJobDispatcherActorTestFixture {
      f =>
        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash", f.schedulerMock.ref, Enqueue))
        val actorRef = f.testKit
          .expectEffectType[Spawned[ScheduledJobProcessorMessage]]
          .ref
        f.schedulerMock.expectMessage(JobReceived)

        f.testKit.run(Dispatch(TestBatchProcessorActorTyped.key, "hash2", f.schedulerMock.ref, Enqueue))
        f.testKit
          .expectEffectType[NoEffects]
        f.schedulerMock.expectMessage(JobEnqueued)

        f.testKit.run(JobStopped(TestBatchProcessorActorTyped.key, actorRef))
        val effect = f.testKit
          .expectEffectType[Spawned[ScheduledJobProcessorMessage]]

        val actorRef2 = effect.ref
        effect.childName shouldEqual s"${TestBatchProcessorActorTyped.key}-hash2"

        f.testKit.run(JobStopped(TestBatchProcessorActorTyped.key, actorRef2))
        f.testKit.expectEffectType[NoEffects]
    }

    "Respond with confusion if key does not exist in map" in withJobDispatcherActorTestFixture { f =>
      val key = "abcasdaw"
      f.testKit.run(Dispatch(key, "hash", f.schedulerMock.ref, RunConcurrent))
      f.testKit.expectEffectType[NoEffects]
      f.schedulerMock.expectMessage(UnrecognizedJob(key))
    }

  }
}
