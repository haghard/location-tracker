package com.rides

import akka.actor.typed.{Behavior, SupervisorStrategy}
import akka.persistence.typed.PersistenceId
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.state.scaladsl.{DurableStateBehavior, Effect}
import com.rides.domain.VehicleCmd
import com.rides.domain.types.protobuf.VehicleStatePB

import scala.concurrent.duration.DurationInt
import com.rides.domain.*
import com.rides.domain.{GetLocation, ReportLocation}

/*
https://softwaremill.com/akka-durable-state/?s=03

https://doc.akka.io/docs/akka/current/typed/durable-state/persistence.html#cluster-sharding-and-durablestatebehavior

https://github.com/akka/akka/blob/main/akka-persistence-typed-tests/src/test/scala/akka/persistence/typed/state/scaladsl/DurableStateBehaviorReplySpec.scala
https://github.com/akka/akka/tree/main/akka-persistence-typed-tests/src/test/scala/akka/persistence/typed/state/scaladsl


https://doc.akka.io/docs/akka-persistence-jdbc/current/durable-state-store.html
https://doc.akka.io/docs/akka/2.6/durable-state/persistence-query.html (DurableStateStoreRegistry)
 */
object VehicleStateBased {

  private val numberOfShards: Int = 1 << 6
  val TypeKey                     = EntityTypeKey[VehicleCmd]("rides")

  final case class ShardingMsgExtractor(
    ua: SelfUniqueAddress,
    shardNum: Int
  ) extends ShardingMessageExtractor[VehicleCmd, VehicleCmd] {
    override def entityId(cmd: VehicleCmd): String =
      cmd match {
        case c: ReportLocation => c.vehicleId.toString
        case _: GetLocation    => throw new Exception("GetLocation.entityId")
        case StopEntity()      => throw new Exception("StopEntity.entityId")
      }

    override def shardId(entityId: String): String =
      math.abs(entityId.hashCode % shardNum).toString

    override def unwrapMessage(cmd: VehicleCmd): VehicleCmd = cmd
  }

  def shardingMessageExtractor(selfAddress: SelfUniqueAddress) =
    ShardingMsgExtractor(selfAddress, numberOfShards)

  def apply(persistenceId: PersistenceId): Behavior[VehicleCmd] =
    Behaviors.setup { ctx =>
      DurableStateBehavior[VehicleCmd, VehicleStatePB](
        persistenceId,
        VehicleStatePB(vehicleId = persistenceId.id.toLong),
        cmdHandler(ctx.log)
      )
        .onPersistFailure(SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
        .receiveSignal {
          case (state, akka.persistence.typed.state.RecoveryCompleted) =>
            ctx.log.warn(s"RecoveryCompleted: [${state.toProtoString}]. SeqNum:${DurableStateBehavior
                .lastSequenceNumber(ctx)}. Raw size: ${state.serializedSize} bts")
          case (state, akka.persistence.typed.state.RecoveryFailed(ex)) =>
            ctx.log.error("RecoveryFailed: ", ex)
        }
    }

  def cmdHandler(logger: org.slf4j.Logger): (VehicleStatePB, VehicleCmd) => Effect[VehicleStatePB] =
    (vehicle, cmd) =>
      cmd match {
        case ReportLocation(id, location, replyTo) =>
          val now = java.time.Instant.now()
          val updatedVehicle = vehicle
            .withVehicleId(id)
            .withLat(location.lat)
            .withLon(location.lon)
            .withUpdatedAt(com.google.protobuf.timestamp.Timestamp.of(now.getEpochSecond(), now.getNano()))
            .withReplyTo(replyTo)

          // Effect.delete[VehicleStatePB]()

          Effect
            .persist(updatedVehicle)
            .thenRun(_ => logger.info("Persist {}", updatedVehicle.toProtoString))
            .thenNoReply() //

        case _: GetLocation =>
          Effect
            .none[VehicleStatePB]
            .thenNoReply()

        case StopEntity() =>
          Effect
            .none[VehicleStatePB]
            .thenRun(_ => logger.info("Passivate"))
            .thenStop()
      }
}
