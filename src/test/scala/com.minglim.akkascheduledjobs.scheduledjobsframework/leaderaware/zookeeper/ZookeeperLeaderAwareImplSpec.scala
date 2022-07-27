package com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeeper

import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper.ZookeeperLeaderAwareSettings
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper.{ZookeeperLeaderAwareImpl, ZookeeperLeaderAwareSettings}
import com.typesafe.config.ConfigFactory
import org.apache.curator.framework.CuratorFramework
import org.mockito.Mockito
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar.mock

class ZookeeperLeaderAwareImplSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  val zkClient    = mock[CuratorFramework]
  val leaderAware = new ZookeeperLeaderAwareImpl(ZookeeperLeaderAwareSettings("localhost:2181", "/randompath", false))

  override def afterAll(): Unit = {
    leaderAware.close()
  }
  "ZookeeperLeaderAwareImpl" should {
    "current machine is not leader if there is no zookeeper service" in {
      leaderAware.isLeader() shouldEqual false
    }
  }

  "ZookeeperLeaderAwareSettings" should {
    "parse the config correctly" in {
      val testConfig           =
        """
          |zookeeper {
          |  url = "test-this:1080"
          |  path = "/akka/cluster/cds/seed/local"
          |  block-until-connected = false
          |}
          |""".stripMargin
      val expectedZookeeperURL = "test-this:1080"
      val expectedPath         = "/akka/cluster/cds/seed/local" + "-v2"
      val expectedBlock        = false
      val actual               = ZookeeperLeaderAwareSettings.apply(ConfigFactory.parseString(testConfig))
      actual shouldEqual ZookeeperLeaderAwareSettings(expectedZookeeperURL, expectedPath, expectedBlock)
    }
  }
}
