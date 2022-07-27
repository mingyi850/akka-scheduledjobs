package com.minglim.akkascheduledjobs.scheduledjobsframework.metrics

trait Message {
  def send(): Unit
}

trait HadoopReporter {
  def send(msg: Message): Unit
}

object NoOpHadoopReporter extends HadoopReporter {
  override def send(msg: Message): Unit = {}
}
