package com.rides.state

import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ApiProcessor
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.persistence.typed.state.scaladsl.Effect
import com.rides.domain.GetLocation
import com.rides.domain.ReportLocation
import com.rides.domain.StopEntity
import com.rides.domain.VehicleCmd
import com.rides.domain.types.protobuf.VehicleStatePB

import scala.concurrent.duration.DurationInt

object Vehicle2 {

  type State = VehicleStatePB

  def apply(p: ApiProcessor, vehicleId: Long): Behavior[VehicleCmd] =
    Behaviors.setup { ctx =>
      // val refResolver = ActorRefResolver(ctx.system)
      DurableStateBehavior[VehicleCmd, State](
        PersistenceId.ofUniqueId(vehicleId.toString),
        VehicleStatePB(vehicleId),
        cmdHandler(p, ctx.log)
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

  def cmdHandler(p: ApiProcessor, logger: org.slf4j.Logger): (State, VehicleCmd) => Effect[State] =
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

          // p.offer((???, cmd))

          Effect
            .persist(updatedVehicle)
            .thenNoReply()

        case _: GetLocation =>
          Effect
            .none[State]
            .thenNoReply()

        case StopEntity() =>
          Effect
            .none[State]
            .thenStop()
      }
}
