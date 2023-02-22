package com.rides

import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.Replicator.UpdateFailure
import akka.cluster.ddata.Replicator.UpdateResponse
import akka.cluster.ddata.Replicator.UpdateSuccess
import akka.cluster.ddata.Replicator.UpdateTimeout

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

object Respondee {

  def apply(
    reqId: String,
    response: Promise[Option[VehicleReply]],
    timeout: FiniteDuration
  ): Behavior[UpdateResponse[_]] = {
    require(timeout > Duration.Zero, s"timeout must be > 0, but was $timeout!")
    Behaviors
      .setup[UpdateResponse[_]] { ctx =>
        val logger = ctx.log
        val start  = System.currentTimeMillis()

        Behaviors.withTimers[UpdateResponse[_]] { timers =>
          timers.startSingleTimer(UpdateTimeout(null, null), timeout)

          Behaviors
            .receiveMessage[UpdateResponse[_]] {
              case UpdateSuccess(_, Some((vehicleId, revision, location))) =>
                logger.warn("[{}] took: {}ms", reqId, System.currentTimeMillis() - start)
                val r = VehicleReply(
                  vehicleId.asInstanceOf[Long],
                  location.asInstanceOf[com.rides.domain.types.protobuf.Location],
                  com.rides.VehicleReply.ReplyStatusCode.MasterAcknowledged,
                  com.rides.VehicleReply.DurabilityLevel.Local,
                  com.rides.Versions(revision.asInstanceOf[Long], revision.asInstanceOf[Long])
                )
                response.trySuccess(Some(r)): Unit
                Behaviors.stopped
              case UpdateTimeout(_, _) =>
                logger.warn("[{}] timeout: {}ms", reqId, System.currentTimeMillis() - start)
                response.trySuccess(None): Unit
                Behaviors.stopped
              case _: UpdateFailure[_] =>
                Behaviors.stopped
            }
            .receiveSignal { case (_, PostStop) =>
              response.trySuccess(None): Unit
              Behaviors.same
            }
        }
      }
      .narrow
  }
}

/*Behaviors.withTimers[VehicleReply] { timers =>
  timers.startSingleTimer(Timeout, timeout)

  val start = System.currentTimeMillis()
  Behaviors
    .receiveMessage[VehicleReply] {
      case Timeout =>
        logger.warn("[{}] timeout: {}ms", reqId, System.currentTimeMillis() - start)
        response.trySuccess(None)
        Behaviors.stopped
      case r: VehicleReply =>
        logger.warn("[{}] took: {}ms", reqId, System.currentTimeMillis() - start)
        response.trySuccess(Some(r))
        Behaviors.stopped
    }
    .receiveSignal { case (_, PostStop) =>
      response.trySuccess(None)
      Behaviors.same
    }
}*/
