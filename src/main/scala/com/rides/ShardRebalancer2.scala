/*
package com.rides

import akka.actor.RootActorPath
import akka.actor.typed.Behavior
import akka.actor.typed.DispatcherSelector
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member

import scala.collection.immutable
import scala.concurrent.duration.DurationInt

object ShardRebalancer {

  object RebalanceTick extends ClusterDomainEvent

  object Shutdown extends ClusterDomainEvent

  final case class ShardAllocated(shards: Set[akka.cluster.sharding.ShardRegion.ShardId]) extends ClusterDomainEvent

  def apply(): Behavior[ClusterDomainEvent] =
    Behaviors.setup[ClusterDomainEvent] { ctx =>
      implicit val ec = ctx.system.dispatchers.lookup(DispatcherSelector.fromConfig("akka.actor.internal-dispatcher"))
      implicit val logger = ctx.log

      val cluster = akka.cluster.typed.Cluster(ctx.system)
      cluster.subscriptions.tell(akka.cluster.typed.Subscribe(ctx.self, classOf[ClusterDomainEvent]))

      val DDataShardReplicatorPath = RootActorPath(cluster.selfMember.address) / "system" / "sharding" / "replicator"
      ctx.system.toClassic
        .actorSelection(DDataShardReplicatorPath)
        .resolveOne(3.seconds)
        .foreach { ddataShardReplicator =>
          akka.cluster.utils
            .shardingStateChanges(
              ctx.self,
              ddataShardReplicator,
              ctx.system,
              cluster.selfMember.address.host.getOrElse("local")
            )
        }(ctx.executionContext)

      Behaviors.withTimers { timer =>
        timer.startTimerWithFixedDelay(RebalanceTick, 25.seconds) // rebalance interval
        active(immutable.SortedSet.from(cluster.state.members)(Member.ageOrdering), Set.empty)(ctx.log)
      }
    }

  def active(
    members: immutable.SortedSet[Member],
    shards: Set[akka.cluster.sharding.ShardRegion.ShardId]
  )(implicit log: org.slf4j.Logger): Behavior[ClusterDomainEvent] =
    Behaviors.receiveMessagePartial {
      case MemberUp(member) =>
        active(members + member, shards)

      case MemberRemoved(member, _) =>
        active(members - member, shards)

      case RebalanceTick =>
        Behaviors.same

      case ShardAllocated(allShards) =>
        active(members, allShards)

      case Shutdown =>
        Behaviors.stopped
    }
}
 */
