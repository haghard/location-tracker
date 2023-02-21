package com.rides

import akka.actor.Address
import akka.actor.RootActorPath
import akka.actor.typed.Behavior
import akka.actor.typed.DispatcherSelector
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.*
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.external.scaladsl.ExternalShardAllocationClient
import com.twitter.hashing.ConsistentHashingDistributor
import com.twitter.hashing.HashNode

import java.security.MessageDigest
import java.util.Base64
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
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
  *
  * Use cases: Colocate Tenant/Users, TaxPayer/Asset/Liability, or StartSchema(Fact/Dimentions)
  */
object ShardRebalancer {

  object RebalanceTick                                                                    extends ClusterDomainEvent
  object Shutdown                                                                         extends ClusterDomainEvent
  final case class ShardAllocated(shards: Set[akka.cluster.sharding.ShardRegion.ShardId]) extends ClusterDomainEvent

  def base64Encode(bs: Array[Byte]): String =
    new String(Base64.getUrlEncoder.withoutPadding.encode(bs))

  def base64Decode(s: String): Option[Array[Byte]] =
    Try(Base64.getUrlDecoder.decode(s)).toOption

  private def updateShardLocations1(
    members: immutable.SortedSet[Member],
    shards: Set[akka.cluster.sharding.ShardRegion.ShardId],
    client: ExternalShardAllocationClient
  )(implicit log: org.slf4j.Logger) = {
    val shardPlacement: Map[ShardId, Address] = {
      val ring = akka.routing.ConsistentHash[Member](Iterable.from(members.iterator), 5)
      shards.foldLeft(Map.empty[ShardId, Address]) { (acc, shardId) =>
        acc + (shardId -> ring.nodeFor(shardId).address)
      }
    }
    println(s"Placement: [${shardPlacement.groupMap(_._2)(_._1).mkString(",")}]")
    client.updateShardLocations(shardPlacement)
  }

  private def updateShardLocations2(
    members: immutable.SortedSet[Member],
    shards: Set[akka.cluster.sharding.ShardRegion.ShardId],
    client: ExternalShardAllocationClient
  )(implicit log: org.slf4j.Logger) = {
    import akka.cluster.Implicits.*
    val shardPlacement: Map[ShardId, Address] = {
      val array = Vector.from(members.iterator)
      log.warn("Shards:[{}]", array.map(_.addressWithIncNum).mkString(","))
      shards.foldLeft(Map.empty[ShardId, Address]) { (acc, shardId) =>
        // If your buckets change from [alpha, bravo, charlie] to [bravo, charlie], it will assign all the old alpha traffic to bravo and all the old bravo traffic to charlie,
        // rather than letting bravo keep its traffic.
        acc + (shardId -> array(com.google.common.hash.Hashing.consistentHash(shardId.toLong, array.size)).address)
      }
    }
    println(s"Placement: [${shardPlacement.groupMap(_._2)(_._1).mkString(",")}]")
    client.updateShardLocations(shardPlacement)
  }

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
        sb.append(it.next().addressWithIncNum).append(",")

      log.warn("UpdateShardLocations: [{}] Shards:[{}]", sb.toString(), shards.mkString(","))
      // To calculate a checksum, a cryptographic hash function like MD5, SHA-1, SHA-256, SHA3-256, or SHA-512 is used
      val membersDigest = MessageDigest.getInstance("SHA3-256").digest(sb.toString().getBytes)
      val array         = Vector.from(members.iterator)
      log.warn("Shards:[{}] Digest:{}", array.map(_.addressWithIncNum).mkString(","), base64Encode(membersDigest))

      shards.foldLeft(Map.empty[ShardId, Address]) { (acc, shardId) =>
        // https://github.com/twitter/util/blob/83bcbe6a857a846b29395bfd720c2985bb2602aa/util-hashing/src/test/scala/com/twitter/hashing/ConsistentHashingDistributorTest.scala
        // 160 is the hard coded value for libmemcached, which was this input data is from

        /*val nodes = Seq(
          HashNode("10.0.1.1", 600, 1),
          HashNode("10.0.1.2", 300, 2),
          HashNode("10.0.1.3", 200, 3),
          HashNode("10.0.1.4", 350, 4),
          HashNode("10.0.1.5", 1000, 5),
          HashNode("10.0.1.6", 800, 6),
          HashNode("10.0.1.7", 950, 7),
          HashNode("10.0.1.8", 100, 8)
        )*/

        val nodes = Vector.from(
          members.iterator.map(member => HashNode(member.details, 100, member))
        )
        val ketamaDistributor = new ConsistentHashingDistributor(nodes, 160)
        val member = ketamaDistributor.nodeForHash(com.twitter.hashing.KeyHasher.KETAMA.hashKey(shardId.getBytes))

        acc + (shardId -> member.address)
      }
    }

    println(s"Placement: [${shardPlacement.groupMap(_._2)(_._1).mkString(",")}]")
    client.updateShardLocations(shardPlacement)
  }

  def apply(
    shardAllocationClient: ExternalShardAllocationClient,
    rebalanceInterval: FiniteDuration
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
        timer.startTimerWithFixedDelay(RebalanceTick, rebalanceInterval) // 60.seconds rebalance interval
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
