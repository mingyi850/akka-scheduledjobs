package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJobsStartupActor.StartupMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.DispatcherMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.{TestBatchProcessorActorTyped, TestBindings}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.JobSchedulerActor.SchedulerMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor.{ScheduleMasterMessage, StartSchedulers}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, TestScheduledJobsContext}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJobsStartupActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.AlwaysLeader
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ScheduledJobsStartupActorTest extends AnyWordSpec with Matchers {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  case class ScheduledJobsStartupActorTestFixture(
      testKit: BehaviorTestKit[StartupMessage],
  )(implicit val scheduledJobsContext: ScheduledJobsContext)

  def withScheduledJobsStartupActorTestFixture(testCode: ScheduledJobsStartupActorTestFixture => Any): Any = {
    implicit val scheduledJobsContext = TestScheduledJobsContext.context
    implicit val leaderAware          = AlwaysLeader
    val bindings                      = TestBindings()
    val jobList                       = List(TestBatchProcessorActorTyped)
    val testKit                       = BehaviorTestKit(ScheduledJobsStartupActor(jobList, bindings))
    testCode(ScheduledJobsStartupActorTestFixture(testKit))
  }

  "ScheduledJobStartupActor" should {
    "Instantiate router and schedule master" in withScheduledJobsStartupActorTestFixture { f =>
      f.testKit
        .expectEffectType[Spawned[DispatcherMessage]]

      f.testKit
        .expectEffectType[Spawned[ScheduleMasterMessage]]
        .ref

      f.testKit.childInbox[ScheduleMasterMessage]("scheduleMaster-actor").expectMessage(StartSchedulers)
    }
  }
}
