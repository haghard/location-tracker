package akka.cluster.sharding

import akka.actor.ActorRef
import akka.actor.Address
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable
import scala.collection.immutable.SortedSet
import scala.concurrent.Future

final class DynamicLeastShardAllocationStrategy2(
  rebalanceThreshold: Int,
  maxSimultaneousRebalance: Int,
  rebalanceNumber: Int,
  rebalanceFactor: Double,
  counter: java.util.concurrent.atomic.AtomicLong = new java.util.concurrent.atomic.AtomicLong(0)
) extends ShardAllocationStrategy
    with Serializable {

  def this(rebalanceThreshold: Int, maxSimultaneousRebalance: Int) =
    this(rebalanceThreshold, maxSimultaneousRebalance, rebalanceThreshold, 0.0)

  override def allocateShard(
    requester: ActorRef,
    shardId: ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]
  ): Future[ActorRef] =
    Future.successful {
      // round robin
      val member = Vector.from(
        SortedSet.from(currentShardAllocations.keySet)((x: ActorRef, y: ActorRef) =>
          Address.addressOrdering.compare(x.path.address, y.path.address)
        )
      )((counter.getAndIncrement() % currentShardAllocations.keySet.size).toInt) // round-robin

      println(s"Allocate ($shardId ---> ${member.path.address}, ${counter.get()})")
      member
    }

  override def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
    rebalanceInProgress: Set[ShardId]
  ): Future[Set[ShardId]] =
    Future.successful {
      if (rebalanceInProgress.size < maxSimultaneousRebalance) {
        val (_, leastShards) = currentShardAllocations.minBy { case (_, v) => v.size }

        val mostShards = currentShardAllocations
          .collect { case (_, v) =>
            v.filterNot(s => rebalanceInProgress(s))
          }
          .maxBy(_.size)

        val difference = mostShards.size - leastShards.size
        if (difference > rebalanceThreshold) {

          val factoredRebalanceLimit = (rebalanceFactor, rebalanceNumber) match {
            // This condition is to maintain semantic backwards compatibility, from when rebalanceThreshold was also
            // the number of shards to move.
            case (0.0, 0)            => rebalanceThreshold
            case (0.0, justAbsolute) => justAbsolute
            case (justFactor, 0)     => math.max((justFactor * mostShards.size).round.toInt, 1)
            case (factor, absolute)  => math.min(math.max((factor * mostShards.size).round.toInt, 1), absolute)
          }

          // The ideal number to rebalance to so these nodes have an even number of shards
          val evenRebalance = difference / 2
          val n = math.min(
            math.min(factoredRebalanceLimit, evenRebalance),
            maxSimultaneousRebalance - rebalanceInProgress.size
          )
          val shards = mostShards.sorted.take(n).toSet
          println(s"Rebalance: [${shards.mkString(",")}]")
          shards
        } else Set.empty[ShardId]
      } else Set.empty[ShardId]
    }
}
