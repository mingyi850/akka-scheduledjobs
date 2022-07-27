package com.minglim.akkascheduledjobs.scheduledjobsframework.actor.typed.listener

import ConfigChangeListenerActor.{ConfigChangeListenerMessage, ConfigChanged}
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.minglim.akkascheduledjobs.scheduledjobsframework.context.{ScheduledJobsContext, StrictScheduledJobsLogging}
import com.typesafe.config.Config

case class ConfigChangeListenerActor[OwnerT, MessageT <: OwnerT](
    ownerRef: ActorRef[OwnerT],
    updateMessageFn: Config => MessageT
)(implicit scheduledJobsContext: ScheduledJobsContext)
    extends StrictScheduledJobsLogging {

  def Listening(ctx: ActorContext[ConfigChangeListenerMessage]): Behavior[ConfigChangeListenerMessage] = {
    scheduledJobsContext.configWatcher.addListenerActor(ownerRef.path.name, ctx.self)
    Behaviors
      .receiveMessage[ConfigChangeListenerMessage] { message =>
        message match {
          case ConfigChanged(config) => ownerRef ! updateMessageFn(config)
        }
        Behaviors.same
      }
      .receiveSignal {
        case (context, PostStop) => {
          logDebug(s"Actor: ${context.self.path.name} has been stopped")
          scheduledJobsContext.configWatcher.removeListenerActor(ownerRef.path.name)
          Behaviors.same
        }
      }
  }

}

object ConfigChangeListenerActor {
  sealed trait ConfigChangeListenerMessage
  case class ConfigChanged(config: Config) extends ConfigChangeListenerMessage
  def apply[OwnerT, MessageT <: OwnerT](ownerRef: ActorRef[OwnerT], updateMessageFn: Config => MessageT)(implicit
      scheduledJobsContext: ScheduledJobsContext
  ): Behavior[ConfigChangeListenerMessage] = {
    Behaviors.setup { ctx =>
      new ConfigChangeListenerActor[OwnerT, MessageT](ownerRef, updateMessageFn).Listening(ctx)
    }
  }
}
