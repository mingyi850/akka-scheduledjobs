package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.{Dispatch, DispatcherMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor.ConfigChangeListenerMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.{TestBatchProcessorActorTyped, TestBindings, TestMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.JobSchedulerActor.{JobAlreadyReceived, JobReceived, SchedulerMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor.{ScheduleMasterMessage, SchedulerConfigChanged, SchedulerStopped, StartSchedulers}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, TestScheduledJobsContext}
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.{AlwaysLeader, AlwaysNotLeader, LeaderAware}
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.JobDedupStrategy.RunConcurrent
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.AnchorTime
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.ScheduledJob
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.{AlwaysLeader, LeaderAware}
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.ScheduleSettings
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.HashGeneratorService
import com.typesafe.config.ConfigFactory
import org.joda.time.LocalDateTime
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ScheduleMasterActorTest extends ScalaTestWithActorTestKit(ManualTime.config) with AnyWordSpecLike with Matchers {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  val manualTime                    = ManualTime()

  case class ScheduleMasterActorTestFixture(
      synchronousTestKit: BehaviorTestKit[ScheduleMasterMessage],
      jobList: List[ScheduledJob[TestBindings]],
      dispatcher: ActorRef[DispatcherMessage]
  )(implicit val scheduledJobsContext: ScheduledJobsContext, val leaderAware: LeaderAware)

  def withScheduleMasterActorTestFixture(testCode: ScheduleMasterActorTestFixture => Any): Any = {
    implicit val scheduledJobsContext = TestScheduledJobsContext.context
    implicit val leaderAware          = AlwaysLeader
    val jobList                       = List(TestBatchProcessorActorTyped)
    val dispatcher                    = TestInbox[DispatcherMessage]().ref
    val synchonousTestKit             = BehaviorTestKit(ScheduleMasterActor(jobList, dispatcher))
    testCode(
      ScheduleMasterActorTestFixture(synchonousTestKit, jobList, dispatcher)
    )
  }

  "ScheduleMasterActor" should {
    "Spawn listener and schedulers for each job when StartSchedulers" in withScheduleMasterActorTestFixture { f =>
      f.synchronousTestKit.run(StartSchedulers)
      f.synchronousTestKit.expectEffectType[Spawned[ConfigChangeListenerMessage]]
      f.synchronousTestKit.expectEffectType[SpawnedAnonymous[SchedulerMessage]]
    }

    "Reschedule when Running if config changes" in withScheduleMasterActorTestFixture { f =>
      case class TestMessageInternal()
      val testProbe                                    = testKit.createTestProbe[SchedulerMessage]()
      val testProbe2                                   = testKit.createTestProbe[TestMessageInternal]()
      val testProbeActor                               = Behaviors.monitor(
        testProbe.ref,
        Behaviors.receiveSignal[SchedulerMessage] {
          case (context, PostStop) => {
            testProbe2 ! TestMessageInternal()

            Behaviors.same
          }
        }
      )
      val masterActor: ActorRef[ScheduleMasterMessage] =
        testKit.spawn(Behaviors.setup { ctx: ActorContext[ScheduleMasterMessage] =>
          new ScheduleMasterActor(ctx, f.jobList, f.dispatcher)(f.scheduledJobsContext, f.leaderAware, ec) {
            val ref                                                                                          = ctx.spawn(testProbeActor, "test-probe-actor")
            override def spawnScheduler(key: String, settings: ScheduleSettings): ActorRef[SchedulerMessage] = ref
          }.Start
        })

      masterActor ! StartSchedulers

      val configStr =
        """
            |scheduledjobs {
            |    test-batch-actor {
            |            batch-size = 30
            |            fetch-size = 100
            |            batch-interval = 5 seconds
            |            retries = 3
            |            fetch-retry-delay = 3 seconds
            |            do-process = true
            |            schedule {
            |                schedule-interval = 10 minutes
            |                anchor-time {
            |                }
            |            }
            |        }
            |
            |}
            |""".stripMargin
      val newConfig = ConfigFactory.parseString(configStr)

      masterActor ! SchedulerConfigChanged(newConfig)
      testProbe2.expectMessage(TestMessageInternal())
    }

    "Start new instance when scheduledStopped" in withScheduleMasterActorTestFixture { f =>
      f.synchronousTestKit.run(StartSchedulers)
      f.synchronousTestKit.expectEffectType[Spawned[ConfigChangeListenerMessage]]
      f.synchronousTestKit.expectEffectType[SpawnedAnonymous[SchedulerMessage]]
      f.synchronousTestKit.run(SchedulerStopped(TestBatchProcessorActorTyped.key))
      f.synchronousTestKit.expectEffectType[SpawnedAnonymous[SchedulerMessage]]
    }

  }
}
