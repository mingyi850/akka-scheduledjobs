package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler

import com.minglim.akkascheduledjobs.scheduledjobsframework.util.DateTimeUtils._
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.{Dispatch, DispatcherMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor.{ScheduleMasterMessage, SchedulerStopped}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.ScheduledJobsMetrics.JobSchedulerMetrics
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.ScheduledJobsMetrics.JobSchedulerMetrics.JobScheduled
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.DispatcherMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.ScheduleMasterActor.ScheduleMasterMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.LeaderAware
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.ScheduleSettings
import com.minglim.akkascheduledjobs.scheduledjobsframework.util.HashGeneratorService
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.{Duration, LocalDateTime, Seconds}

class JobSchedulerActor(
    key: String,
    scheduleSettings: ScheduleSettings,
    timer: TimerScheduler[JobSchedulerActor.SchedulerMessage],
    dispatcherActor: ActorRef[DispatcherMessage],
    schedulerMaster: ActorRef[ScheduleMasterMessage],
    hashGeneratorService: HashGeneratorService,
    initialDelay: Option[FiniteDuration] = None
)(implicit
    scheduledJobsContext: ScheduledJobsContext,
    leaderAware: LeaderAware,
    ec: ExecutionContext
) extends StrictScheduledJobsLogging {
  import JobSchedulerActor._

  val ACK_TIMEOUT            = Math.min((scheduleSettings.scheduleInterval.toSeconds / 2), 5).seconds
  var nextRun: LocalDateTime = scheduleSettings.getNextStartTime(initialDelay = initialDelay)
  val schedule               =
    timer.startTimerWithFixedDelay(StartJob, StartJob, getTimeToNextRun(nextRun), scheduleSettings.scheduleInterval)

  logInfo(s"$key - Starting now with scheduleSettings as: ${scheduleSettings}")
  private def WaitingNext(): Behavior[SchedulerMessage] = {
    logInfo(
      s"$key - Waiting Next now - nextRun: ${nextRun} in " +
        s"${getTimeToNextRun(nextRun)}"
    )
    Behaviors
      .receive[SchedulerMessage] { (context, message) =>
        message match {
          case StartJob  => {
            if (leaderAware.isLeader()) {
              logInfo(s"$key - Scheduler is leader. Calling to dispatch Job")
              val hash = dispatchHash(key, nextRun)
              dispatcherActor ! Dispatch(key, hash, context.self, scheduleSettings.jobDedupStrategy)
              nextRun = nextRun.plus(Duration.standardSeconds(scheduleSettings.scheduleInterval.toSeconds))
              JobSchedulerMetrics.JobScheduled(key).report()
              WaitingAck(hash)
            } else {
              logInfo(s"$key - Scheduler is not leader. Will not dispatch Job")
              nextRun = nextRun.plus(Duration.standardSeconds(scheduleSettings.scheduleInterval.toSeconds))
              Behaviors.same
            }
          }
          case unhandled => {
            logWarn(s"$key - Message ${unhandled} not able to be handled in this state:")
            Behaviors.same
          }
        }
      }
      .receiveSignal(handleSignal)
  }

  private def WaitingAck(hash: String): Behavior[SchedulerMessage] = {
    logDebug(s"$key - Waiting Ack now")
    timer.startSingleTimer(AckTimeout, AckTimeout, ACK_TIMEOUT)
    Behaviors
      .receive[SchedulerMessage] { (context, message) =>
        message match {
          case JobReceived | JobAlreadyReceived | JobDropped => {
            logDebug(s"$key -Got acknowledgement")
            timer.cancel(AckTimeout)
            WaitingNext()
          }
          case AckTimeout                                    => {
            logDebug(s"$key - Got AckTimeout")
            JobSchedulerMetrics.SchedulerAckTimeout(key).report()
            dispatcherActor ! Dispatch(key, hash, context.self, scheduleSettings.jobDedupStrategy)
            WaitingAck(hash)
          }
          case unhandled                                     => {
            logWarn(s"$key - Message ${unhandled} not able to be handled in this state:")
            Behaviors.same
          }
        }
      }
      .receiveSignal(handleSignal)
  }

  private def getTimeToNextRun(nextRun: LocalDateTime): FiniteDuration = {
    Seconds.secondsBetween(LocalDateTime.now(), nextRun).getSeconds.seconds
  }
  private[scheduler] def dispatchHash(key: String, nextRun: LocalDateTime): String = {
    hashGeneratorService.generateHash(key, nextRun.toString)
  }
  private[scheduler] def updateNextRun: Unit = {
    nextRun = nextRun.plus(Duration.standardSeconds(scheduleSettings.scheduleInterval.toSeconds))
  }

  private def handleSignal: PartialFunction[(ActorContext[SchedulerMessage], Signal), Behavior[SchedulerMessage]] = {
    case (context: ActorContext[SchedulerMessage], PostStop) => {
      logInfo(s"$key - Actor: ${context.self.path.name} has been stopped")
      schedulerMaster ! SchedulerStopped(key)
      Behaviors.same
    }
  }

}

object JobSchedulerActor {

  val PREFIX = "job-scheduler"
  sealed trait SchedulerMessage
  sealed trait WaitingNextMessage         extends WaitingAckMessage
  sealed trait WaitingAckMessage          extends SchedulerMessage
  case object StartJob                    extends WaitingNextMessage
  case object JobReceived                 extends WaitingAckMessage
  case object JobDropped                  extends WaitingAckMessage
  case object JobAlreadyReceived          extends WaitingAckMessage
  case object JobEnqueued                 extends WaitingAckMessage
  case object AckTimeout                  extends WaitingAckMessage
  case class UnrecognizedJob(key: String) extends WaitingAckMessage

  def apply(
      key: String,
      scheduleSettings: ScheduleSettings,
      dispatcher: ActorRef[DispatcherMessage],
      schedulerMaster: ActorRef[ScheduleMasterMessage],
      hashGeneratorService: HashGeneratorService,
      initialDelay: Option[FiniteDuration] = None
  )(implicit
      scheduledJobsContext: ScheduledJobsContext,
      leaderAware: LeaderAware,
      ec: ExecutionContext
  ): Behavior[SchedulerMessage] = {
    Behaviors.withTimers(timer =>
      new JobSchedulerActor(
        key,
        scheduleSettings,
        timer,
        dispatcher,
        schedulerMaster,
        hashGeneratorService,
        initialDelay
      )
        .WaitingNext()
    )
  }
}
