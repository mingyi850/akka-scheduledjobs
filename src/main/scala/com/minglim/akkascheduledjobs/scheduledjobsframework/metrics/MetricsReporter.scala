package com.minglim.akkascheduledjobs.scheduledjobsframework.metrics

trait MetricsReporter {
  def report(metric: String, value: Long, tags: Map[String, String]): Unit
}

object NoOpMetricsReporter extends MetricsReporter {
  override def report(metric: String, value: Long, tags: Map[String, String]): Unit = {}
}
