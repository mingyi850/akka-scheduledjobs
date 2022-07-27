package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.{Dispatch, DispatcherMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.JobSchedulerActor.{JobAlreadyReceived, JobDropped, JobReceived}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor.ScheduleMasterMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, TestScheduledJobsContext}
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.{AlwaysLeader, AlwaysNotLeader, LeaderAware}
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.JobDedupStrategy.RunConcurrent
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.AnchorTime
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.JobSchedulerActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.{AlwaysLeader, AlwaysNotLeader, LeaderAware}
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.{AnchorTime, ScheduleSettings}
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.HashGeneratorService
import org.joda.time.LocalDateTime
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class JobSchedulerActorTest extends ScalaTestWithActorTestKit(ManualTime.config) with AnyWordSpecLike with Matchers {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  val manualTime                    = ManualTime()

  case class JobSchedulerActorTestFixture(
      dispatcherActor: TestProbe[DispatcherMessage],
      masterActor: TestProbe[ScheduleMasterMessage],
      scheduleSettings: ScheduleSettings,
      hashGeneratorService: HashGeneratorService,
      leaderAware: LeaderAware,
      key: String
  )(implicit val scheduledJobsContext: ScheduledJobsContext)

  def withJobSchedulerActorTestFixture(testCode: JobSchedulerActorTestFixture => Any): Any = {
    implicit val scheduledJobsContext = TestScheduledJobsContext.context
    val key                           = "randomkey"
    val dispatcher                    = testKit.createTestProbe[DispatcherMessage]
    val masterActor                   = testKit.createTestProbe[ScheduleMasterMessage]
    val leaderAware                   = AlwaysLeader
    val scheduleSettings              = new ScheduleSettings(10.seconds, None)
    val hashGeneratorService          = new HashGeneratorService {
      var counter = 0
      override def generateHash(base: String, randomizer: String): String = {
        counter += 1
        base + counter.toString
      }
    }
    testCode(
      JobSchedulerActorTestFixture(dispatcher, masterActor, scheduleSettings, hashGeneratorService, leaderAware, key)
    )
  }

  "JobSchedulerActor" should {
    "Start telling dispatcher to dispatch and fixed intervals if no anchor time if Leader" in withJobSchedulerActorTestFixture {
      f =>
        val actor = testKit.spawn(
          JobSchedulerActor(
            f.key,
            f.scheduleSettings,
            f.dispatcherActor.ref,
            f.masterActor.ref,
            f.hashGeneratorService
          )(
            f.scheduledJobsContext,
            f.leaderAware,
            ec
          )
        )

        f.dispatcherActor.within(500.millis) {
          f.dispatcherActor.expectMessage(Dispatch(f.key, s"${f.key}1", actor.ref, RunConcurrent))
          actor ! JobReceived
        }
        manualTime.expectNoMessageFor(9.seconds, f.dispatcherActor)
        manualTime.timePasses(1.seconds)
        f.dispatcherActor.expectMessage(Dispatch(f.key, s"${f.key}2", actor.ref, RunConcurrent))

        actor ! JobReceived

        manualTime.expectNoMessageFor(9.seconds, f.dispatcherActor)
        manualTime.timePasses(2.seconds)
        f.dispatcherActor.expectMessage(Dispatch(f.key, s"${f.key}3", actor.ref, RunConcurrent))
    }

    "Start telling dispatcher to dispatch only at anchor time" in withJobSchedulerActorTestFixture { f =>
      val anchorTime       = new AnchorTime(None, None, None, None) {
        override def getNextAnchor(from: LocalDateTime): LocalDateTime =
          LocalDateTime.now().plusHours(1).plusMinutes(20).plusSeconds(10)
      }
      val modifiedSettings = f.scheduleSettings.copy(anchorTime = Some(anchorTime))
      val actor            = testKit.spawn(
        JobSchedulerActor(f.key, modifiedSettings, f.dispatcherActor.ref, f.masterActor.ref, f.hashGeneratorService)(
          f.scheduledJobsContext,
          f.leaderAware,
          ec
        )
      )
      manualTime.expectNoMessageFor(80.minutes, f.dispatcherActor)
      manualTime.timePasses(11.seconds)
      f.dispatcherActor.expectMessage(Dispatch(f.key, s"${f.key}1", actor.ref, RunConcurrent))

      actor ! JobAlreadyReceived

      manualTime.expectNoMessageFor(9.seconds, f.dispatcherActor)
      manualTime.timePasses(2.seconds)
      f.dispatcherActor.expectMessage(Dispatch(f.key, s"${f.key}2", actor.ref, RunConcurrent))
    }

    "Re-send request to schedule if no ackowledgement is received" in withJobSchedulerActorTestFixture { f =>
      val actor = testKit.spawn(
        JobSchedulerActor(f.key, f.scheduleSettings, f.dispatcherActor.ref, f.masterActor.ref, f.hashGeneratorService)(
          f.scheduledJobsContext,
          f.leaderAware,
          ec
        )
      )
      f.dispatcherActor.within(500.millis) {
        f.dispatcherActor.expectMessage(Dispatch(f.key, s"${f.key}1", actor.ref, RunConcurrent))
      }

      manualTime.expectNoMessageFor(4.seconds, f.dispatcherActor)
      manualTime.timePasses(2.seconds)
      f.dispatcherActor.expectMessage(Dispatch(f.key, s"${f.key}1", actor.ref, RunConcurrent))

      manualTime.expectNoMessageFor(4.seconds, f.dispatcherActor)
      manualTime.timePasses(2.seconds)
      f.dispatcherActor.expectMessage(Dispatch(f.key, s"${f.key}1", actor.ref, RunConcurrent))
    }

    "Not Telling dispatcher to dispatch if not Leader" in withJobSchedulerActorTestFixture { f =>
      val actor = testKit.spawn(
        JobSchedulerActor(
          f.key,
          f.scheduleSettings,
          f.dispatcherActor.ref,
          f.masterActor.ref,
          f.hashGeneratorService
        )(
          f.scheduledJobsContext,
          AlwaysNotLeader,
          ec
        )
      )

      manualTime.expectNoMessageFor(10.hours, f.dispatcherActor)
    }

  }
}
