package com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware

import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.{AlwaysLeader, AlwaysNotLeader}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LeaderAwareSpec extends AnyWordSpec with Matchers {

  "AlwaysLeader" should {
    "always returns the leader as true" in {
      AlwaysLeader.isLeader() shouldEqual true
    }
  }

  "AlwaysNotLeader" should {
    "always returns the leader as false" in {
      AlwaysNotLeader.isLeader() shouldEqual false
    }
  }
}
