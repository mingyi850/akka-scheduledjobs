package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed

import akka.actor.typed.Behavior
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.dispatcher.JobDispatcherActor.DispatcherMessage
import com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.processor.BatchProcessActorTyped.ProcessorContext
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.ScheduledJobsContext

import scala.concurrent.ExecutionContext

abstract class ScheduledJobActor {
  def dispatch(implicit processorContext: ProcessorContext): Behavior[ScheduledJobProcessorMessage]
}

trait ScheduledJob[BindingsT] {
  def key: String
  def create(bindings: BindingsT)(implicit
      scheduledJobsContext: ScheduledJobsContext,
      ec: ExecutionContext
  ): ScheduledJobActor
}

trait ScheduledJobProcessorMessage
