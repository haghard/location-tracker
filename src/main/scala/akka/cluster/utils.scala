package akka
package cluster

import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.cluster.ddata.{LWWMap, LWWMapKey, LWWRegister, LWWRegisterKey, Replicator}
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.stream.{ActorAttributes, CompletionStrategy, KillSwitch, KillSwitches, Materializer, OverflowStrategy, Supervision}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.typed.scaladsl.ActorSource
import com.rides.ShardRebalancer
import com.rides.state.Vehicle

import scala.util.control.NonFatal

object utils {

  //
  // We need it to subscribe to it
  /** It's a copy of existing key that's being used in [[akka.cluster.sharding.DDataShardCoordinator]] for
    * LWWRegister[akka.cluster.sharding.ShardCoordinator.Internal.State]
    */

  // shard name
  val typeName: String = Vehicle.TypeKey.name

  val CoordinatorStateKey =
    LWWRegisterKey[akka.cluster.sharding.ShardCoordinator.Internal.State](s"${typeName}CoordinatorState")

  // https://doc.akka.io/docs/akka/current/typed/cluster-sharding.html#inspecting-cluster-sharding-state
  def shardingStateChanges(
    rebalancer: akka.actor.typed.ActorRef[ShardRebalancer.ShardAllocated],
    ddataShardReplicator: ActorRef,
    sys: ActorSystem[_],
    selfHost: String
  ): KillSwitch = {

    implicit val m = Materializer.matFromSystem(sys)
    val actorWatchingFlow =
      Flow[String]
        .watch(ddataShardReplicator)
        .buffer(1 << 2, OverflowStrategy.backpressure)

    type ShardCoordinatorState = LWWRegister[akka.cluster.sharding.ShardCoordinator.Internal.State]
    val (actorSource, src) = ActorSource
      .actorRef[Replicator.SubscribeResponse[ShardCoordinatorState]](
        completionMatcher = { case _: Replicator.Deleted[ShardCoordinatorState] => CompletionStrategy.draining },
        failureMatcher = PartialFunction.empty,
        1 << 2,
        OverflowStrategy.dropHead
      )
      .preMaterialize()

    ddataShardReplicator ! Replicator.Subscribe(CoordinatorStateKey, actorSource.toClassic)

    src
      .collect { case value @ Replicator.Changed(_) =>
        val state = value.get(CoordinatorStateKey).value

        rebalancer.tell(ShardRebalancer.ShardAllocated(state.allShards))

        new StringBuilder()
          .append("\n")
          // .append("Shards: [")
          // .append(state.shards.keySet.mkString(","))
          // .append(state.shards.mkString(","))
          // .append(state.shards.map { case (k, ar) => s"$k:${ar.path.address.host.getOrElse(selfHost)}" }.mkString(","))
          // .append("]")
          // .append("\n")
          .append(s"ShardCoordinatorState($selfHost) updated [ ")
          .append(
            state.regions
              .map { case (sr, shards) => s"${sr.path.address.host.getOrElse(selfHost)}:[${shards.mkString(",")}]" }
              .mkString(", ")
          )
          .append(" ]")
          .toString()
      }
      .via(actorWatchingFlow)
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.foreach(stateLine => sys.log.warn(stateLine)))
      .withAttributes(
        ActorAttributes.supervisionStrategy {
          case ex: akka.stream.WatchedActorTerminatedException =>
            sys.log.error("Replicator failed. Terminate stream", ex)
            Supervision.Stop
          case NonFatal(ex) =>
            sys.log.error("Unexpected error!", ex)
            Supervision.Stop
        }
      )
      .run()
  }

  def stateChanges(
    externalShardingReplicator: ActorRef,
    sys: ActorSystem[_]
  ): KillSwitch = {
    implicit val m = Materializer.matFromSystem(sys)
    val actorWatchingFlow =
      Flow[String]
        .watch(externalShardingReplicator)
        .buffer(1, OverflowStrategy.backpressure)

    type ExternalShardAllocationState = LWWMap[ShardId, String]
    val Key: LWWMapKey[ShardId, String] = ExternalShardAllocationStrategy.ddataKey(typeName)

    val (actorSource, src) = ActorSource
      .actorRef[Replicator.SubscribeResponse[ExternalShardAllocationState]](
        completionMatcher = { case _: Replicator.Deleted[ExternalShardAllocationState] =>
          CompletionStrategy.draining
        },
        failureMatcher = PartialFunction.empty,
        1,
        OverflowStrategy.dropHead
      )
      .preMaterialize()

    externalShardingReplicator ! Replicator.Subscribe(Key, actorSource.toClassic)

    src
      .collect { case value @ Replicator.Changed(_) =>
        new StringBuilder()
          .append("\n")
          .append(s"ExternalShardingState(${value.get(Key).entries.groupMap(_._2)(_._1).mkString(", ")})")
          .append("\n")
          .toString()
      }
      .via(actorWatchingFlow)
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.foreach(stateLine => sys.log.warn(stateLine)))
      .withAttributes(
        ActorAttributes.supervisionStrategy {
          case ex: akka.stream.WatchedActorTerminatedException =>
            sys.log.error("Replicator failed. Terminate stream", ex)
            Supervision.Stop
          case NonFatal(ex) =>
            sys.log.error("Unexpected error!", ex)
            Supervision.Stop
        }
      )
      .run()
  }
}
