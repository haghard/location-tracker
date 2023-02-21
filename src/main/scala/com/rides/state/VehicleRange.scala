package com.rides.state

import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.persistence.typed.state.scaladsl.Effect
import com.rides.domain.*
import com.rides.domain.types.protobuf.VehicleRangeStatePB
import com.rides.domain.types.protobuf.VehicleStatePB

import scala.concurrent.duration.DurationInt

/** Range-based partitioning.
  *
  * All requests for a given range of vehicles i.e ([1..10] or [11...20]) will land on the same range
  *
  * We divide vehicle's data among shards by key ranges, which means all reads and writes for a single range will be
  * directed at a single shard. Also it allows us to control how big an individual shard can get. In our case, we have
  * up to 10 entities per shard.
  *
  * https://martinfowler.com/articles/patterns-of-distributed-systems/key-range-partitions.html#CalculatingPartitionSizeAndFindingTheMiddleKey
  * Predefining key ranges
  */
object VehicleRange {

  sealed trait OrderState
  case object APPROVED         extends OrderState
  case object APPROVAL_PENDING extends OrderState
  case object REVISION_PENDING extends OrderState
  case object REJECTED         extends OrderState

  /*
  import static net.chrisrichardson.ftgo.orderservice.api.events.OrderState.APPROVED;
  import static net.chrisrichardson.ftgo.orderservice.api.events.OrderState.APPROVAL_PENDING;
  import static net.chrisrichardson.ftgo.orderservice.api.events.OrderState.REJECTED;
  import static net.chrisrichardson.ftgo.orderservice.api.events.OrderState.REVISION_PENDING;
   */

  type State = VehicleRangeStatePB

  val TypeKey = EntityTypeKey[VehicleCmd]("vehicles")

  val numOfShards = 100 / 10

  // akka://rides/system/sharding/vehicles/0.10/0.10
  // akka://rides/system/sharding/vehicles/10.20/10.20

  // akka://rides/system/sharding/vehicles/0/0 - [0..9]
  // akka://rides/system/sharding/vehicles/10/10 - [10..19]

  // TODO: ????? Sorted ranges
  // akka://rides/system/sharding/vehicles/0.10/0..5
  // akka://rides/system/sharding/vehicles/0.10/6..10

  // Hash based
  // shards:   [0 .. 10]
  // entities (vNodes): [0...10],[11...20],[21...31]
  // akka://rides/system/sharding/vehicles/1/11

  def shardingMessageExtractor() =
    new ShardingMessageExtractor[VehicleCmd, VehicleCmd] {
      val shards = (0 to 100).by(10).toVector

      // val ring = akka.routing.ConsistentHash[Int](Iterable.range(1, 10), 1)
      // ring.nodeFor("")

      /*def nameAsShardName(vehicleId: String): String =
        shards.search(vehicleId.toInt).insertionPoint match {
          case 0                         => s"0.${shards(0)}"
          case i if i == shards.size - 1 => s"${shards(i)}.105"
          case ind                       => s"${shards(ind - 1)}.${shards(ind)}"
        }*/

      // abs(id.hashCode % maxNumberOfShards)

      def shardName(vehicleId: String): String =
        shards.search(vehicleId.toInt).insertionPoint match {
          case 0                         => "0"
          case i if i == shards.size - 1 => shards(i).toString
          case ind                       => shards(ind - 1).toString
        }

      override def entityId(cmd: VehicleCmd): String =
        cmd match {
          case c: ReportLocation => shardName(c.vehicleId.toString)
          case _: GetLocation    => throw new Exception("GetLocation.entityId")
          case StopEntity()      => throw new Exception("StopEntity.entityId")
        }

      override def shardId(entityId: String): String = entityId

      override def unwrapMessage(cmd: VehicleCmd): VehicleCmd = cmd
    }

  def apply(persistenceId: PersistenceId): Behavior[VehicleCmd] =
    Behaviors.setup { ctx =>
      DurableStateBehavior[VehicleCmd, State](
        persistenceId,
        VehicleRangeStatePB(),
        cmdHandler(ctx)
      )
        .onPersistFailure(SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
        .receiveSignal {
          case (state, akka.persistence.typed.state.RecoveryCompleted) =>
            ctx.log.warn(
              s"RecoveryCompleted: [${state.toProtoString}]. " +
                s"SeqNum:${DurableStateBehavior.lastSequenceNumber(ctx)}. Raw size: ${state.serializedSize} bts"
            )
          case (state, akka.persistence.typed.state.RecoveryFailed(ex)) =>
            ctx.log.error("RecoveryFailed: ", ex)
        }
    }

  def cmdHandler(ctx: ActorContext[VehicleCmd]): (State, VehicleCmd) => Effect[State] =
    (state, cmd) => {
      val logger: org.slf4j.Logger = ctx.log
      cmd match {
        case ReportLocation(id, location, respondee /*, requestId*/ ) =>
          val now = java.time.Instant.now()
          val updatedVehicle = state.vehicles
            .getOrElse(id, VehicleStatePB(vehicleId = id))
            .withLat(location.lat)
            .withLon(location.lon)
            .withUpdatedAt(com.google.protobuf.timestamp.Timestamp.of(now.getEpochSecond(), now.getNano()))

          val updatedVehicles = state
            .copy(vehicles = state.vehicles + (id -> updatedVehicle))
            .withUpdatedVehicleId(id) // this id is current being updated
            .withReplyTo(respondee)

          Effect
            .persist(updatedVehicles)
            .thenRun(_ => logger.warn("★ ★ ★ Persist [{}]", updatedVehicles.vehicles.keySet.mkString(",")))
            .thenNoReply() //

        case _: GetLocation =>
          Effect
            .none[State]
            .thenNoReply()

        case StopEntity() =>
          Effect
            .none[State]
            .thenRun(_ => logger.info("Passivate"))
            .thenStop()
      }
    }
}
