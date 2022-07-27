package com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware

trait LeaderAware {
  def isLeader(): Boolean
}

case object AlwaysLeader extends LeaderAware {
  override def isLeader(): Boolean = true
}

case object AlwaysNotLeader extends LeaderAware {
  override def isLeader(): Boolean = false
}
