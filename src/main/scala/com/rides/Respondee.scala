package com.rides

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.rides.VehicleReply

import scala.concurrent.Promise
import scala.concurrent.duration.{Duration, FiniteDuration}

object Respondee {

  private val Timeout =
    VehicleReply(
      Long.MinValue,
      com.rides.domain.types.protobuf.Location(),
      VehicleReply.ReplyStatusCode.Timeout
    )

  def apply(name: String, response: Promise[Option[VehicleReply]], timeout: FiniteDuration): Behavior[VehicleReply] = {
    require(timeout > Duration.Zero, s"timeout must be > 0, but was $timeout!")
    Behaviors
      .setup[VehicleReply] { ctx =>
        val logger = ctx.log

        Behaviors.withTimers { timers =>
          timers.startSingleTimer(Timeout, timeout)

          val start = System.currentTimeMillis()
          Behaviors.receiveMessage {
            case Timeout =>
              logger.warn("[{}] Timeout-Took: {}ms", name, System.currentTimeMillis() - start)
              response.trySuccess(None)
              Behaviors.stopped
            case r: VehicleReply =>
              logger.warn("[{}] Took: {}ms", name, System.currentTimeMillis() - start)
              response.trySuccess(Some(r))
              Behaviors.stopped
          }
        }
      }
      .narrow
  }
}
