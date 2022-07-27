package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.{Dispatch, DispatcherMessage, JobCompleted, JobStopped, PendingJob, RunningJob}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.BatchProcessActorTyped.ProcessorContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.scheduler.JobSchedulerActor.{JobAlreadyReceived, JobDropped, JobEnqueued, JobReceived, SchedulerMessage, UnrecognizedJob}
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.ScheduledJobsMetrics.DispatcherMetrics
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.ScheduledJobsMetrics.DispatcherMetrics.JobRunDuration
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.{ScheduledJob, ScheduledJobProcessorMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.JobDedupStrategy.{Drop, Enqueue, JobDedupStrategy, RunConcurrent}
import org.joda.time.LocalDateTime

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class JobDispatcherActor[BindingsT](jobList: List[ScheduledJob[BindingsT]], bindings: BindingsT)(implicit
    scheduledJobsContext: ScheduledJobsContext,
    ec: ExecutionContext
) extends StrictScheduledJobsLogging {
  private val keyMap                                                                  = jobList.map(job => (job.key, job)).toMap
  private val currentJobsByKey: scala.collection.mutable.Map[String, Set[RunningJob]] =
    mutable.HashMap.empty[String, Set[RunningJob]] ++ keyMap.keys.map(key => (key, Set[RunningJob]())).toMap

  private val pendingJobsByKey: scala.collection.mutable.Map[String, List[PendingJob]] =
    mutable.HashMap.empty[String, List[PendingJob]]

  def Receiving(): Behavior[DispatcherMessage] = {
    logDebug(s"Starting now with keymap: ${keyMap}")
    Behaviors.receive { (context, command) =>
      {
        logDebug(s"Received command ${command}")
        command match {
          case Dispatch(key, hash, replyTo, strategy) => {
            logDebug(s"Got key: ${key} with job from map as ${keyMap.get(key)}")
            keyMap.get(key) match {
              case Some(job) =>
                strategy match {
                  case RunConcurrent if jobAlreadyRunning(key, hash, checkHash = true)                             => {
                    DispatcherMetrics.JobDropped(key, "hash-dedup").report()
                    replyTo ! JobAlreadyReceived
                  }
                  case RunConcurrent                                                                               => {
                    DispatcherMetrics.JobDispatched(key).report()
                    startJob(context, job, key, hash, Some(replyTo))
                  }
                  case Drop if jobAlreadyRunning(key, hash, checkHash = true)                                      => {
                    DispatcherMetrics.JobDropped(key, "hash-dedup").report()
                    replyTo ! JobAlreadyReceived
                  }
                  case Drop if jobAlreadyRunning(key, hash, checkHash = false)                                     => {
                    DispatcherMetrics.JobDropped(key, "already-running").report()
                    replyTo ! JobDropped
                  }
                  case Drop                                                                                        => {
                    DispatcherMetrics.JobDispatched(key)
                    startJob(context, job, key, hash, Some(replyTo))
                  }
                  case Enqueue if (jobAlreadyRunning(key, hash, checkHash = true) || jobAlreadyPending(key, hash)) =>
                    DispatcherMetrics.JobDropped(key, "hash-dedup").report()
                    replyTo ! JobAlreadyReceived
                  case Enqueue if jobAlreadyRunning(key, hash, checkHash = false)                                  => {
                    addPendingJob(key, hash)
                    DispatcherMetrics.JobEnqueued(key).report()
                    DispatcherMetrics.ConcurrentJobs(key, pendingJobsByKey(key).size).report()
                    replyTo ! JobEnqueued
                  }
                  case Enqueue                                                                                     => {
                    DispatcherMetrics.JobDispatched(key)
                    startJob(context, job, key, hash, Some(replyTo))
                  }
                  case _                                                                                           => {
                    logWarn("No strategy matching for this message")
                    replyTo ! JobDropped
                  }
                }

              case None =>
                logError(
                  "No such key exists, check that you are providing a consistent key between all settings and objects"
                )
                replyTo ! UnrecognizedJob(key)
            }
          }
          case JobCompleted(key, ref)                 => {
            logDebug(s"Job completed: ${ref.path.name}")
          }
          case JobStopped(key, ref)                   => {
            logDebug(s"Job stopped: ${ref.path.name}")
            DispatcherMetrics.JobStopped(key).report()
            removeRunningJob(key, ref)
            startPendingJob(key, context)
          }
        }
      }
      Behaviors.same
    }
  }

  private def startPendingJob(key: String, context: ActorContext[DispatcherMessage]) = {
    val pendingJobs = pendingJobsByKey.get(key).getOrElse(List.empty)
    for {
      pendingJob <- pendingJobs.headOption
      job        <- keyMap.get(key)
    } yield {
      startJob(context, job, pendingJob.key, pendingJob.hash)
      removePendingJob(pendingJob.key, pendingJob.hash)
    }
  }

  private def startJob(
      context: ActorContext[DispatcherMessage],
      job: ScheduledJob[BindingsT],
      key: String,
      hash: String,
      replyTo: Option[ActorRef[SchedulerMessage]] = None
  ) = {
    val actorRef =
      context.spawn(job.create(bindings).dispatch(ProcessorContext(job.key, context.self)), s"$key-$hash")
    addRunningJob(key, hash, actorRef)
    replyTo.map(ref => ref ! JobReceived)
  }

  private def jobAlreadyRunning(key: String, hash: String, checkHash: Boolean): Boolean = {
    currentJobsByKey.get(key).map(set => if (checkHash) set.exists(_.hash == hash) else set.nonEmpty).getOrElse(false)
  }

  private def jobAlreadyPending(key: String, hash: String): Boolean = {
    pendingJobsByKey.get(key).map(list => list.exists(_.hash == hash)).getOrElse(false)
  }
  private def addRunningJob(key: String, hash: String, actorRef: ActorRef[ScheduledJobProcessorMessage]) = {
    val newJob = RunningJob(key, hash, actorRef, LocalDateTime.now())
    currentJobsByKey.update(key, currentJobsByKey(key) + newJob)
    logDebug(s"JobDispatcher: CurrentRunningJobs: ${currentJobsByKey}")
  }

  private def removeRunningJob(key: String, actorRef: ActorRef[ScheduledJobProcessorMessage]) = {
    currentJobsByKey.get(key).foreach { ls =>
      val job = ls.filter(job => job.actorRef == actorRef)
      currentJobsByKey.update(key, ls -- job)
      job.foreach(j => DispatcherMetrics.JobRunDuration(j).report())
    }
    logDebug(s"JobDispatcher: CurrentRunningJobs: ${currentJobsByKey}")
  }
  private def addPendingJob(key: String, hash: String) = {
    val newJob = PendingJob(key, hash, LocalDateTime.now())
    pendingJobsByKey.update(key, pendingJobsByKey.get(key).getOrElse(List.empty) :+ newJob)
    logDebug(s"JobDispatcher: CurrentPendingJobs: ${pendingJobsByKey}")
  }

  private def removePendingJob(key: String, hash: String) = {
    pendingJobsByKey.get(key).foreach { ls =>
      val job = ls.filter(job => job.hash == hash)
      pendingJobsByKey.update(key, ls.filter(_.hash != hash))
    }
    logDebug(s"JobDispatcher: CurrentPendingJobs: ${pendingJobsByKey}")
  }
}

object JobDispatcherActor {
  sealed trait DispatcherMessage
  case class Dispatch(
      key: String,
      hash: String,
      replyTo: ActorRef[SchedulerMessage],
      jobDedupStrategy: JobDedupStrategy
  )                                                                                    extends DispatcherMessage
  case class JobCompleted(key: String, jobRef: ActorRef[ScheduledJobProcessorMessage]) extends DispatcherMessage
  case class JobStopped(key: String, jobRef: ActorRef[ScheduledJobProcessorMessage])   extends DispatcherMessage

  def apply[BindingsT](dispatchList: List[ScheduledJob[BindingsT]], bindings: BindingsT)(implicit
      scheduledJobsContext: ScheduledJobsContext,
      ec: ExecutionContext
  ): Behavior[DispatcherMessage] = {
    new JobDispatcherActor(dispatchList, bindings).Receiving()
  }
  case class RunningJob(
      key: String,
      hash: String,
      actorRef: ActorRef[ScheduledJobProcessorMessage],
      started: LocalDateTime
  )

  case class PendingJob(
      key: String,
      hash: String,
      enqueued: LocalDateTime
  )
}
