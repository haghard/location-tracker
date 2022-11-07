package com.rides

import akka.actor.{Address, RootActorPath}
import akka.actor.typed.{Behavior, DispatcherSelector}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent.{ClusterDomainEvent, MemberRemoved, MemberUp}
import akka.cluster.Member
import akka.cluster.sharding.external.scaladsl.ExternalShardAllocationClient

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import akka.cluster.sharding.ShardRegion.ShardId
import akka.actor.typed.scaladsl.adapter.*

import java.security.MessageDigest
import java.util.Base64
import scala.util.Try

/** https://bartoszsypytkowski.com/hash-partitions/s
  * https://github.com/haghard/stateful-akka-streams-examples/blob/master/src/main/scala/examples/Example3KafkaSharding.scala
  * https://github.com/akka/akka-samples/blob/c2b8e6c91c0a3d4c1f1d409a7be2688358eb6baa/akka-sample-kafka-to-sharding-scala/processor/src/main/scala/sample/sharding/kafka/UserEvents.scala
  *
  * https://ringpop.readthedocs.io/en/latest/architecture_design.html#consistent-hashing
  * https://martinfowler.com/articles/patterns-of-distributed-systems/fixed-partitions.html
  *
  * We want to control where shards go using consistent hashing.
  *
  * We leverage consistent hashing to minimize the number of keys to rebalance when your cluster is resized. Consistent
  * hashing allows the nodes to rebalance themselves with traffic evenly distributed.
  * [[com.google.common.hash.Hashing.consistentHash]] hashing function is fast and provides good distribution.
  * Consistent hashing applies a hash function to not only the identity of your data, but also the nodes within your
  * cluster that are operating on that data.
  */
object ShardRebalancer {

  object RebalanceTick                                                                    extends ClusterDomainEvent
  object Shutdown                                                                         extends ClusterDomainEvent
  final case class ShardAllocated(shards: Set[akka.cluster.sharding.ShardRegion.ShardId]) extends ClusterDomainEvent

  def base64Encode(bs: Array[Byte]): String =
    new String(Base64.getUrlEncoder.withoutPadding.encode(bs))

  def base64Decode(s: String): Option[Array[Byte]] =
    Try(Base64.getUrlDecoder.decode(s)).toOption

  private def updateShardLocations(
    members: immutable.SortedSet[Member],
    shards: Set[akka.cluster.sharding.ShardRegion.ShardId],
    client: ExternalShardAllocationClient
  )(implicit log: org.slf4j.Logger) = {

    val shardPlacement: Map[ShardId, Address] = {

      import akka.cluster.Implicits.*

      val sb = new StringBuilder()
      val it = members.iterator
      while (it.hasNext)
        sb.append(it.next().addressWithNum).append(",")

      log.warn("UpdateShardLocations: [{}] Shards:[{}]", sb.toString(), shards.mkString(","))
      val membersDigest = MessageDigest.getInstance("SHA3-256").digest(sb.toString().getBytes)

      val array = Vector.from(members.iterator)
      log.warn("Shards:[{}] Digest:{}", array.map(_.addressWithNum).mkString(","), base64Encode(membersDigest))

      // val ring = akka.routing.ConsistentHash[Member](Iterable.from(members.iterator), 5)
      shards.foldLeft(Map.empty[ShardId, Address]) { (acc, shardId) =>
        // acc + (shardId -> ring.nodeFor(shardId).address)

        // If your buckets change from [alpha, bravo, charlie] to [bravo, charlie], it will assign all the old alpha traffic to bravo and all the old bravo traffic to charlie,
        // rather than letting bravo keep its traffic.
        acc + (shardId -> array(com.google.common.hash.Hashing.consistentHash(shardId.toLong, members.size)).address)
      }
    }

    // println(s"Placement: [${shardPlacement.groupMap(_._2)(_._1).mkString(",")}]")

    client.updateShardLocations(shardPlacement)
  }

  def apply(
    shardAllocationClient: ExternalShardAllocationClient
  ): Behavior[ClusterDomainEvent] =
    Behaviors.setup[ClusterDomainEvent] { ctx =>
      implicit val ec = ctx.system.dispatchers.lookup(DispatcherSelector.fromConfig("akka.actor.internal-dispatcher"))
      implicit val logger = ctx.log

      val cluster = akka.cluster.typed.Cluster(ctx.system)
      cluster.subscriptions.tell(akka.cluster.typed.Subscribe(ctx.self, classOf[ClusterDomainEvent]))

      /** Looks up the replicator that's being used by [[akka.cluster.sharding.DDataShardCoordinator]]
        */
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

      /*val ExtShardingReplicator = RootActorPath(cluster.selfMember.address) / "system" / "DDataStateActor"
      ctx.system.toClassic
        .actorSelection(DDataShardReplicatorPath)
        .resolveOne(3.seconds)
        .foreach { extShardingReplicator =>
          akka.cluster.utils.stateChanges(
            extShardingReplicator,
            ctx.system
          )
        }*/

      Behaviors.withTimers { timer =>
        timer.startTimerWithFixedDelay(RebalanceTick, 60.seconds) // rebalance interval
        active(immutable.SortedSet.from(cluster.state.members)(Member.ageOrdering), Set.empty)(
          shardAllocationClient,
          ctx.log,
          ctx.executionContext
        )
      }
    }

  def active(
    members: immutable.SortedSet[Member],
    shards: Set[akka.cluster.sharding.ShardRegion.ShardId]
  )(implicit
    client: ExternalShardAllocationClient,
    log: org.slf4j.Logger,
    ec: ExecutionContext
  ): Behavior[ClusterDomainEvent] =
    Behaviors.receiveMessagePartial {
      case MemberUp(member) =>
        active(members + member, shards)

      case MemberRemoved(member, _) =>
        active(members - member, shards)

      case RebalanceTick =>
        if (members.nonEmpty && shards.nonEmpty)
          updateShardLocations(
            members,
            shards,
            client
          )
        Behaviors.same

      case ShardAllocated(allShards) =>
        if (members.nonEmpty && allShards.nonEmpty)
          updateShardLocations(
            members,
            allShards,
            client
          )
        active(members, allShards)

      case Shutdown =>
        Behaviors.stopped
    }
}
