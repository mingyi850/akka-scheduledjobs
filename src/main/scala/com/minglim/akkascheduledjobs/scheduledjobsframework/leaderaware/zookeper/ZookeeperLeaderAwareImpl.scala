package com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.zookeper

import com.minglim.akkascheduledjobs.scheduledjobsframework.leaderaware.LeaderAware
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.{CancelLeadershipException, LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.retry.ExponentialBackoffRetry

import java.util.concurrent.atomic.AtomicBoolean

class ZookeeperLeaderAwareImpl(zookeeperLeaderElectionSettings: ZookeeperLeaderAwareSettings)
    extends LeaderAware
    with StrictLogging
    with AutoCloseable {

  logger.info(s"using zookeeper for leader election with $zookeeperLeaderElectionSettings ...")

  private val isSelfLeader = new AtomicBoolean(false)

  private[zookeper] val zkClient = CuratorFrameworkFactory.newClient(
    zookeeperLeaderElectionSettings.zookeeperURL,
    15000,
    15000,
    new ExponentialBackoffRetry(
      1000,
      30
    )
  )
  zkClient.start()
  if (zookeeperLeaderElectionSettings.blockUntilConnected) {
    zkClient.blockUntilConnected()
  }

  private val leaderSelector = new LeaderSelector(
    zkClient,
    zookeeperLeaderElectionSettings.zookeeperLeaderElectionPath,
    new LeaderSelectorListenerAdapter() {
      override def takeLeadership(client: CuratorFramework): Unit = {
        logger.warn(s"I'm the new leader ${Math.random()}")
        isSelfLeader.set(true)
        Thread.currentThread().join() // hold leadership
        isSelfLeader.set(false)
        logger.warn("giving up the leadership")
      }
      override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
        if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {
          logger.warn(s"The leader state is changed. I'll try to remove leadership from myself.")
          isSelfLeader.set(false)
          throw new CancelLeadershipException()
        }
      }
    }
  )
  leaderSelector.autoRequeue()
  leaderSelector.start()

  override def isLeader(): Boolean = isSelfLeader.get()

  override def close(): Unit = {
    leaderSelector.close()
    zkClient.close()
  }
}

case class ZookeeperLeaderAwareSettings(
    zookeeperURL: String,
    zookeeperLeaderElectionPath: String,
    blockUntilConnected: Boolean
)

object ZookeeperLeaderAwareSettings {

  def apply(config: Config): ZookeeperLeaderAwareSettings =
    new ZookeeperLeaderAwareSettings(
      zookeeperURL = config.getString("zookeeper.url"),
      zookeeperLeaderElectionPath = s"${config.getString("zookeeper.path")}-v2",
      blockUntilConnected = config.getBoolean("zookeeper.block-until-connected")
    )
}
