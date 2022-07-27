package com.minglim.akkascheduledjobs.scheduledjobsframework.model

object JobDedupStrategy extends Enumeration {
  type JobDedupStrategy = Value
  val RunConcurrent = Value("run-concurrent")
  val Drop          = Value("drop")
  val Enqueue       = Value("enqueue")
}
