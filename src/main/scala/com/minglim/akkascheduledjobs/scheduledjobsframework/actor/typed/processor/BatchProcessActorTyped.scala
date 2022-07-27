package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor

import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.{ScheduledJobActor, ScheduledJobProcessorMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.{DispatcherMessage, JobCompleted, JobStopped}
import BatchProcessActorTyped._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.ScheduledJobsMetrics.BatchProcessActorMetrics
import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.ScheduledJobsMetrics.BatchProcessActorMetrics.{CommitDuration, FetchDuration, ProcessDuration}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.{ScheduledJobActor, ScheduledJobProcessorMessage}
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener.ConfigChangeListenerActor
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.minglim.akkascheduledjobs.scheduledjobsframework.settings.BatchProcessActorSettings
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

abstract class BatchProcessActorTyped[Req, Res <: BatchResult[
  Req
], SettingsT <: BatchProcessActorSettings[SettingsT]](
    batchProcessActorSettings: SettingsT
)(implicit
    scheduledJobsContext: ScheduledJobsContext,
    ec: ExecutionContext
) extends ScheduledJobActor
    with StrictScheduledJobsLogging {

  // API to implement
  /** Mandatory: User-defined method to fetch all Requests/Items which need to be processed. User should implement their own logic to split requests into batches if required. */
  def fetchAllBatches(): Future[List[Req]]

  /** Mandatory: User defined logic to process each batch. */
  def processBatch(req: Req): Future[Res]

  /** Mandatory: User should provide a default failed instance of Result type to return in case of failure or any exceptions */
  def batchRecover(req: Req): PartialFunction[Throwable, Res]

  /** Optional: User defined logic to commit batch upon completion or failure. This logic can be subsumed in processing logic as well, but its generally advised to handle it here */
  def commitBatch(res: Res): Future[Unit] = Future.successful(())

  /** Optional: User can provide a side effect to be executed upon fetch completed */
  def handleFetchComplete(requests: List[Req]): Unit = logDebug(s"fetched: $requests")

  /** Optional: User can provide a side effect to be executed upon batch completion */
  def handleBatchComplete(res: Res): Unit = logDebug(s"completed batch with result: $res")

  /** Optional: User can provide a side effect to be executed upon batch committed */
  def handleBatchCommitted(res: Res): Unit = logDebug(s"completed batch with result: $res")

  /** Optional: User can provide a side effect to be executed upon the actor being forcefully stopped */
  def handleProcessStopped(): Unit = logDebug(s"Process stopped. Converting remaining requests into failures.")

  /** Optional: User can provide a side effect to be executed upon completion of the job */
  def handleProcessComplete(): Unit =
    logDebug(s"Remaining Requests are: ${requestList.toList}, Results are: ${resultsList.toList}")

  private val resultsList        = scala.collection.mutable.ListBuffer[Res]()
  private val requestList        = scala.collection.mutable.Queue[RequestWithRetry[Req]]()
  var currentSettings: SettingsT = batchProcessActorSettings

  def getAllResults        = resultsList.toList
  def getRemainingRequests = requestList.toList

  override final def dispatch(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] = {
    Behaviors.setup { ctx =>
      ctx.spawn(ConfigChangeListenerActor(ctx.self, newConfig => ProcessorConfigChanged(newConfig)), "config-listener")
      Starting(Some(ScheduledEvent(0.seconds, FetchJobBatches(currentSettings.retries))))
    }
  }

  final def Starting(
      scheduledEventOpt: Option[ScheduledEvent] = None
  )(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] = {
    logDebug("Starting now")
    behaviorWithTimeout(currentSettings.timeoutSettings.fetchTimeout)(scheduledEventOpt) {
      case (context, message: StartingEvent) =>
        message match {
          case FetchJobBatches(retries: Int)         => {
            logDebug("Got FetchJob Batches")
            context.pipeToSelf(FetchDuration(processorContext.key).measureAsync(fetchAllBatches())) {
              case Success(result)           => SetupJob(result)
              case Failure(e) if retries > 0 => FetchJobFailed(retries)
              case Failure(e)                => {
                logDebug("No more retries for fetch, shutting down")
                Shutdown
              }
            }
            Behaviors.same
          }
          case FetchJobFailed(remainingRetries: Int) => {
            logDebug(s"Got fetch Job failed with retries: ${remainingRetries}")
            BatchProcessActorMetrics.BatchesFetched(processorContext.key, 0, false).report()
            Starting(Some(ScheduledEvent(currentSettings.fetchRetryDelay, FetchJobBatches(remainingRetries - 1))))
          }
          case SetupJob(requests: List[Req])         => {
            logDebug(s"Setup Job with num requests: ${requests.size}")
            BatchProcessActorMetrics.BatchesFetched(processorContext.key, requests.size, true).report()
            requests.foreach(req => requestList.enqueue(RequestWithRetry(req, currentSettings.retries)))
            handleFetchComplete(requests)
            context.self ! RequestNext
            WaitingRequest()
          }
          case Timeout                               => {
            BatchProcessActorMetrics.StateTimeout(processorContext.key, "Starting").report()
            context.self ! FetchJobBatches(currentSettings.retries)
            Behaviors.same
          }
        }
    }

  }

  final def WaitingRequest(
      scheduledEventOpt: Option[ScheduledEvent] = None
  )(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] = {
    logDebug("WaitingRequest now")
    behaviorWithTimeout(currentSettings.timeoutSettings.waitTimeout)(scheduledEventOpt) {
      case (context, message: WaitingEvent) =>
        message match {
          case RequestNext =>
            logDebug(s"Got RequestNext. Taking from internal list size: ${requestList.size}")
            requestList.headOption match {
              case None    =>
                context.self ! HandleProcessComplete
                Finishing()
              case Some(_) => {
                val requestWithRetry = requestList.dequeue()
                if (currentSettings.doProcess) {
                  context.pipeToSelf(
                    ProcessDuration(processorContext.key).measureAsync(processBatch(requestWithRetry.request))
                  ) {
                    case Success(result) => RequestComplete(result, requestWithRetry.retries)
                    case Failure(e)      =>
                      RequestComplete(batchRecover(requestWithRetry.request)(e), requestWithRetry.retries)
                  }
                  Processing()
                } else scheduleNextBatch
              }
            }
          case Timeout     =>
            logDebug(s"Receive Timeout on Waiting Trying to get another Request")
            BatchProcessActorMetrics.StateTimeout(processorContext.key, "WaitingRequest").report()
            context.self ! RequestNext
            Behaviors.same
        }
    }

  }

  final def Processing(
      scheduledEventOpt: Option[ScheduledEvent] = None
  )(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] = {
    logDebug("Processing now")
    behaviorWithTimeout(currentSettings.timeoutSettings.processTimeout)(scheduledEventOpt) {
      case (context, message: ProcessingEvent) =>
        message match {
          case RequestComplete(result: Res, retries) =>
            logDebug(s"Got RequestComplete with result: ${result}")
            handleBatchComplete(result)
            (result, retries) match {
              case (result, retries) if result.isSuccess                  => {
                BatchProcessActorMetrics.BatchProcessed(processorContext.key, true, retries > 0).report()
                commitBatchResult(context, result, currentSettings.retries)
              }
              case (result, retries) if !result.isSuccess && retries <= 0 => {
                BatchProcessActorMetrics.BatchProcessed(processorContext.key, false, false).report()
                commitBatchResult(context, result, currentSettings.retries)
              }
              case (result, retries) if !result.isSuccess && retries > 0  => {
                BatchProcessActorMetrics.BatchProcessed(processorContext.key, false, true).report()
                requestList.enqueue(RequestWithRetry(result.request, retries - 1))
                scheduleNextBatch
              }
            }
          case Timeout                               =>
            logDebug(s"Got Receive timeout while processing")
            BatchProcessActorMetrics.StateTimeout(processorContext.key, "Processing").report()
            scheduleNextBatch
        }
    }
  }

  final def Committing(
      scheduledEventOpt: Option[ScheduledEvent] = None
  )(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] = {
    logDebug("Committing job completion now")
    behaviorWithTimeout(currentSettings.timeoutSettings.commitTimeout)(scheduledEventOpt) {
      case (context, message: CommittingEvent) =>
        message match {
          case RequestCommitted(result: Res)                                  =>
            logDebug(s"Got RequestCommitted with result: ${result}")
            BatchProcessActorMetrics.BatchCommitted(processorContext.key, true, false).report()
            handleBatchCommitted(result)
            resultsList.append(result)
            scheduleNextBatch
          case RequestCommitFailed(result: Res, retries: Int) if retries > 0  =>
            logDebug(s"Got RequestCommitFailed with result: ${result}, and ${retries} retries")
            BatchProcessActorMetrics.BatchCommitted(processorContext.key, false, true).report()
            handleBatchCommitted(result)
            commitBatchResult(context, result, retries - 1)
          case RequestCommitFailed(result: Res, retries: Int) if retries <= 0 =>
            logDebug(s"Got RequestCommitFailed with result: ${result}, no more retries")
            BatchProcessActorMetrics.BatchCommitted(processorContext.key, false, false).report()
            handleBatchCommitted(result)
            resultsList.append(result)
            scheduleNextBatch
          case Timeout                                                        =>
            logDebug(s"Got Receive timeout while committing")
            BatchProcessActorMetrics.StateTimeout(processorContext.key, "Committing").report()
            scheduleNextBatch
        }
    }
  }

  final def Finishing(
      scheduledEventOpt: Option[ScheduledEvent] = None
  )(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] = {
    logDebug("Finishing now")
    behaviorWithTimeout(currentSettings.timeoutSettings.finishTimeout)(scheduledEventOpt) { case (context, message) =>
      message match {
        case HandleProcessComplete =>
          logDebug("Got Handle results. Handling results and shutting down")
          BatchProcessActorMetrics.ProcessCompleted(processorContext.key).report()
          handleProcessComplete()
          processorContext.dispatcher ! JobCompleted(processorContext.key, context.self)
          Behaviors.stopped
        case HandleProcessStopped  =>
          logDebug("Process stopped suddenly")
          handleProcessStopped()
          Behaviors.stopped
        case Timeout               =>
          logDebug("Got receive timeout")
          BatchProcessActorMetrics.StateTimeout(processorContext.key, "Finishing").report()
          context.self ! HandleProcessComplete
          Behaviors.same
      }
    }
  }
  private def behaviorWithTimeout(timeout: FiniteDuration)(scheduledEventOpt: Option[ScheduledEvent] = None)(
      onMessage: PartialFunction[(ActorContext[ScheduledJobProcessorMessage], ScheduledJobProcessorMessage), Behavior[
        ScheduledJobProcessorMessage
      ]]
  )(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] = {
    Behaviors.withTimers[ScheduledJobProcessorMessage] { timers =>
      timers.startSingleTimer(Timeout, timeout)
      scheduledEventOpt.map(scheduledEvent => timers.startSingleTimer(scheduledEvent.event, scheduledEvent.delay))
      Behaviors
        .receivePartial {
          onMessage.orElse(handleShutdown).orElse(handleSettingsChanged)
        }
        .receiveSignal {
          case (context, PostStop) => {
            processorContext.dispatcher ! JobStopped(processorContext.key, context.self)
            Behaviors.same
          }
        }
    }
  }

  private def commitBatchResult(
      context: ActorContext[ScheduledJobProcessorMessage],
      result: Res,
      retries: Int
  )(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] = {
    context.pipeToSelf(CommitDuration(processorContext.key).measureAsync(commitBatch(result)).map(_ => result)) {
      case Success(result) => RequestCommitted(result)
      case Failure(e)      => RequestCommitFailed(result, retries)
    }
    Committing()
  }

  private def scheduleNextBatch(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage] =
    WaitingRequest(Some(ScheduledEvent(currentSettings.batchInterval, RequestNext)))

  private def handleShutdown(implicit
      processorContext: ProcessorContext
  ): PartialFunction[(ActorContext[ScheduledJobProcessorMessage], ScheduledJobProcessorMessage), Behavior[
    ScheduledJobProcessorMessage
  ]] = { case (_, Shutdown) =>
    BatchProcessActorMetrics.ProcessStopped(processorContext.key).report()
    Finishing(Some(ScheduledEvent(0.second, HandleProcessStopped)))
  }

  private def handleSettingsChanged(implicit
      processorContext: ProcessorContext
  ): PartialFunction[(ActorContext[ScheduledJobProcessorMessage], ScheduledJobProcessorMessage), Behavior[
    ScheduledJobProcessorMessage
  ]] = { case (_, ProcessorConfigChanged(config)) =>
    onSettingsChange(config)
    Behaviors.same
  }

  def onSettingsChange(config: Config)(implicit processorContext: ProcessorContext): Unit = {
    val newConfig = Try(currentSettings.fetchNewSettings(config.getConfig(s"scheduledjobs.${processorContext.key}")))
    newConfig match {
      case Success(newConfigValue) if (currentSettings.isChanged(newConfigValue)) => {
        logInfo(s"Config has changed, new config value is: ${newConfigValue}")
        currentSettings = newConfigValue
      }
      case Success(_)                                                             => logInfo(s"No changes to config. Keeping current config $currentSettings")
      case Failure(exp)                                                           =>
        logError(
          s"Unable to create new config from updated config due to $exp keeping current config $currentSettings"
        )
    }
  }

}

object BatchProcessActorTyped {

  trait RequestBatchResult[Req] {
    def isSuccess: Boolean
    def request: Req
  }

  sealed trait BatchProcessActorEvent extends ScheduledJobProcessorMessage
  sealed trait StartingEvent          extends BatchProcessActorEvent
  sealed trait WaitingEvent           extends BatchProcessActorEvent
  sealed trait ProcessingEvent        extends BatchProcessActorEvent
  sealed trait CommittingEvent        extends BatchProcessActorEvent
  sealed trait FinishingEvent         extends BatchProcessActorEvent

  case object Start                                                                 extends StartingEvent
  private[processor] case class FetchJobBatches(retries: Int)                       extends StartingEvent
  private[processor] case class FetchJobFailed(retries: Int)                        extends StartingEvent
  private[processor] case class SetupJob[Req](requestList: List[Req])               extends StartingEvent
  private[processor] case object RequestNext                                        extends WaitingEvent
  private[processor] case class RequestComplete[Res](result: Res, retries: Int)     extends ProcessingEvent
  private[processor] case class RequestCommitted[Res](result: Res)                  extends CommittingEvent
  private[processor] case class RequestCommitFailed[Res](result: Res, retries: Int) extends CommittingEvent
  private[processor] case object HandleProcessComplete                              extends FinishingEvent
  private[processor] case object HandleProcessStopped                               extends FinishingEvent
  case object Shutdown                                                              extends BatchProcessActorEvent
  case class ProcessorConfigChanged(config: Config)                                 extends BatchProcessActorEvent
  private[processor] case object Timeout
      extends StartingEvent
      with WaitingEvent
      with ProcessingEvent
      with CommittingEvent
      with FinishingEvent

  case class ScheduledEvent(delay: FiniteDuration, event: BatchProcessActorEvent)

  abstract class BatchResult[Req](success: Boolean, req: Req) {
    def isSuccess: Boolean = success
    def request: Req       = req
  }

  case class RequestWithRetry[Req](request: Req, retries: Int)

  case class ProcessorContext(key: String, dispatcher: ActorRef[DispatcherMessage])

  object ActorJobStatus extends Enumeration {
    type ActorJobStatus = String
    val Created   = "Created"
    val Running   = "Running"
    val Completed = "Completed"
    val Paused    = "Paused"
    val Cancelled = "Cancelled"
  }
}
