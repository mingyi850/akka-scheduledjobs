package com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeeper

import com.minglim.akkascheduledjobs.scheduledjobsframework.metrics.MetricsReporter
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper.{ConsulDCLeaderAwareSettings, ConsulDCZookeeperLeaderAwareImpl, ZookeeperLeaderAwareImpl}
import com.minglim.akkascheduledjobs.scheduledjobsframework.configwatcher.ScheduledJobsNoOpConfigWatcher
import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper.{ConsulDCLeaderAwareSettings, ConsulDCZookeeperLeaderAwareImpl, ZookeeperLeaderAwareImpl}
import com.minglim.akkascheduledjobs.scheduledjobsframework.model.DC
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar.mock

class ConsulDCZookeeperLeaderAwareImplSpec extends AnyWordSpec with Matchers {

  case class ConsulDCLeaderAwareFixture(
      metricsReporter: MetricsReporter,
      zkLeaderAware: ZookeeperLeaderAwareImpl,
      leaderAware: ConsulDCZookeeperLeaderAwareImpl,
      activeConfig: Config,
      inactiveConfig: Config
  )

  def withFixture(code: ConsulDCLeaderAwareFixture => Any): Any = {
    val activeConfig =
      ConfigFactory.parseString("""
                                  |dc-leader-aware {
                                  |  active-dc = hkg
                                  |}
                                  |""".stripMargin)

    val inactiveConfig =
      ConfigFactory.parseString("""
                                  |dc-leader-aware {
                                  |  active-dc = sgp
                                  |}
                                  |""".stripMargin)

    val settings        = ConsulDCLeaderAwareSettings(activeConfig)
    val metricsReporter = mock[MetricsReporter]
    val zkLeaderAware   = mock[ZookeeperLeaderAwareImpl]
    val dc              = DC.HKG
    val configWatcher   = ScheduledJobsNoOpConfigWatcher
    val leaderAware     =
      new ConsulDCZookeeperLeaderAwareImpl(dc, zkLeaderAware, settings, configWatcher)(metricsReporter)

    val fixture = ConsulDCLeaderAwareFixture(metricsReporter, zkLeaderAware, leaderAware, activeConfig, inactiveConfig)
    code(fixture)
  }

  "current machine is leader if zookeeper and consul config both true" in withFixture { fx =>
    when(fx.zkLeaderAware.isLeader()).thenReturn(true)
    fx.leaderAware.isLeader() shouldEqual true
  }

  "current machine is not leader if zookeeper is false and dc is-active is true" in withFixture { fx =>
    when(fx.zkLeaderAware.isLeader()).thenReturn(false)
    fx.leaderAware.isLeader() shouldEqual false
  }

  "current machine is not leader if zookeeper is true and dc is-active is false" in withFixture { fx =>
    when(fx.zkLeaderAware.isLeader()).thenReturn(true)
    fx.leaderAware.onSettingsChange(fx.inactiveConfig)
    fx.leaderAware.isLeader() shouldEqual false
  }

  "current machine is not leader if zookeeper is false and dc is-active is false" in withFixture { fx =>
    when(fx.zkLeaderAware.isLeader()).thenReturn(false)
    fx.leaderAware.onSettingsChange(fx.inactiveConfig)
    fx.leaderAware.isLeader() shouldEqual false
  }

}
