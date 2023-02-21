package akka.cluster.sharding

import akka.actor.ActorRef
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId

import scala.concurrent.Future

/*

4 goes down

ShardCoordinatorState(172.17.0.8) updated [
 172.17.0.4:[51,35,57,47,53,38,52,45,44,56,37,49,36], 172.17.0.8:[40,46,48,42,41,34,33,55,50,54,43,39],
 172.17.0.6:[]
]

ShardCoordinatorState(172.17.0.8) updated [
  172.17.0.4:[51,35,57,47,53,38,52,56,37,49,36], 172.17.0.8:[40,33,46,48,42,41,34,55,50,54,43,39],
  172.17.0.6:[45]
]


ShardCoordinatorState(172.17.0.8) updated [
  172.17.0.4:[], 172.17.0.8:[40,33,50,46,48,39,42,41,34,55,54,43],
  172.17.0.6:[35,38,45,56]
]

ShardCoordinatorState(172.17.0.8) updated [
  172.17.0.4:[], 172.17.0.8:[40,46,48,42,41,34,33,55,50,54,43,39],
  172.17.0.6:[51,35,57,47,53,38,52,45,44,56,37,49,36]
]

ShardCoordinatorState(172.17.0.8) updated [
  172.17.0.8:[40,46,48,42,41,34,33,55,50,54,43,39],
  172.17.0.6:[51,35,57,47,53,38,52,45,44,56,37,49,36]
]

ShardCoordinatorState(172.17.0.8) updated [ 172.17.0.8:[40,46,48,42,41,34,33,55,50,54,43,39], 172.17.0.6:[51,35,57,47,53,38,52,45,44,56,37,49,36], 172.17.0.9:[] ]

 */

final class AllocationStrategyLogger(
  strategy: akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy, // akka.cluster.sharding.internal.LeastShardAllocationStrategy,
  // strategy: akka.cluster.sharding.DynamicLeastShardAllocationStrategy,
  system: akka.actor.typed.ActorSystem[_]
) extends ShardAllocationStrategy {

  private val delegate = strategy.asInstanceOf[akka.cluster.sharding.internal.LeastShardAllocationStrategy]

  delegate.start(system.classicSystem)

  // Rebalance is also disabled during rolling updates, since shards from stopped nodes are anyway supposed to be started
  // on new nodes. Messages to shards that were stopped on the old nodes will allocate corresponding shards on the new nodes,
  // without waiting for rebalance actions.

  /** Invoked periodically to decide which shards to rebalance to another location.
    *
    * @param currentShardAllocations
    *   all actor refs to `ShardRegion` and their current allocated shards, in the order they were allocated
    * @param rebalanceInProgress
    *   set of shards that are currently being rebalanced, i.e. you should not include these in the returned set
    * @return
    *   a `Future` of the shards to be migrated, may be empty to skip rebalance in this round
    */
  override def rebalance(
    currentShardAllocations: Map[ActorRef, IndexedSeq[ShardId]],
    rebalanceInProgress: Set[ShardId]
  ): Future[Set[ShardId]] =
    delegate
      .rebalance(currentShardAllocations, rebalanceInProgress)
      .map { shards =>
        system.log.warn(s"★ ★ ★ Rebalance [${shards.mkString(",")}] ★ ★ ★")
        shards
      }(system.executionContext)

  /** Invoked when the location of a new shard is to be decided.
    *
    * @param requester
    *   actor reference to the [[ShardRegion]] that requested the location of the shard, can be returned if preference
    *   should be given to the node where the shard was first accessed
    * @param shardId
    *   the id of the shard to allocate
    * @param currentShardAllocations
    *   all actor refs to `ShardRegion` and their current allocated shards, in the order they were allocated
    * @return
    *   a `Future` of the actor ref of the [[ShardRegion]] that is to be responsible for the shard, must be one of the
    *   references included in the `currentShardAllocations` parameter
    */
  override def allocateShard(
    requester: ActorRef,
    shardId: ShardId,
    currentShardAllocations: Map[ActorRef, IndexedSeq[ShardId]]
  ): Future[ActorRef] = {
    system.log.warn(s"Allocate [$shardId]")
    delegate.allocateShard(requester, shardId, currentShardAllocations)
  }
}
