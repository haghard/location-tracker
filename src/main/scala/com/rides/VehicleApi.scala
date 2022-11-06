package com.rides

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem}
import akka.stream.QueueOfferResult.Dropped
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.*
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.control.NoStackTrace
import VehicleApi.*
import akka.grpc.GrpcServiceException
import com.rides.domain.{GetLocation, ReportLocation, StopEntity, VehicleCmd}
import io.grpc.Status
import akka.cluster.ddata.Replicator.{Get, GetFailure, GetSuccess, NotFound, ReadMajority}
import akka.cluster.ddata.replicator.ReplicatedVehicle
import akka.pattern.ask

import akka.cluster.ddata.Replicator.UpdateResponse

object VehicleApi {

  final case class OverCapacity(name: String) extends Exception(s"$name cannot accept more requests") with NoStackTrace

  final case class Error(cause: QueueOfferResult) extends Exception(s"Unexpected $cause!") with NoStackTrace

  def apply(
    shardRegion: ActorRef[VehicleCmd],
    stateReplicator: akka.actor.ActorRef,
    name: String,
    bufferSize: Int,
    parallelism: Int,
    askTimeout: akka.util.Timeout
  )(implicit sys: ActorSystem[_]): VehicleApi =
    new VehicleApi(shardRegion, stateReplicator, name, bufferSize, parallelism)(askTimeout, sys)
}

final class VehicleApi private (
  shardRegion: ActorRef[VehicleCmd],
  stateReplicator: akka.actor.ActorRef,
  name: String,
  bufferSize: Int,
  parallelism: Int
)(implicit
  askTimeout: akka.util.Timeout,
  system: ActorSystem[_]
) {

  require(askTimeout.duration > Duration.Zero, s"timeout for processor $name must be > 0, but was $askTimeout!")
  require(bufferSize > 0, s"bufferSize for processor $name must be > 0, but was $bufferSize!")

  val logger           = system.log
  val actorRefResolver = ActorRefResolver(system)
  val readMajority     = ReadMajority(askTimeout.duration)

  private val (queue, done) =
    Source
      .queue[(ReportLocation, Promise[Option[VehicleReply]], ActorRef[UpdateResponse[_]])](bufferSize)
      .via(
        Flow[(ReportLocation, Promise[Option[VehicleReply]], ActorRef[UpdateResponse[_]])]
          .withAttributes(Attributes.inputBuffer(0, 0))
          .mapAsync(parallelism) { case (cmd, p, respondee) =>
            cmd match {
              case cmd: ReportLocation =>
                shardRegion.tell(cmd.withReplyTo(actorRefResolver.toSerializationFormat(respondee)))
                p.future
            }
          }
          .named("vehicle-api")
      )
      .toMat(Sink.ignore)(Keep.both)
      .addAttributes(ActorAttributes.supervisionStrategy(resume)) // triggers if mapAsyncUnordered(f) fails
      .run()

  CoordinatedShutdown(system)
    .addTask(CoordinatedShutdown.PhaseServiceRequestsDone, s"shutdown-processor-$name") { () =>
      logger.info(s"★ ★ ★ CoordinatedShutdown [api.proc.$name.shutdown]  ★ ★ ★")
      shutdown()
      whenDone
    }

  def askApi(
    reqId: String,
    cmd: VehicleCmd
  ): Future[VehicleReply] =
    cmd match {
      case post: ReportLocation =>
        val p = Promise[Option[VehicleReply]]()
        val respondee: ActorRef[akka.cluster.ddata.Replicator.UpdateResponse[_]] =
          system.systemActorOf(ReplicatorRespondee(reqId, p, askTimeout.duration), reqId)

        queue.offer((post, p, respondee)) match {
          case Enqueued =>
            p.future.flatMap(
              _.fold[Future[VehicleReply]](
                Future.failed(
                  new GrpcServiceException(Status.UNAVAILABLE.withDescription(s"No response within $askTimeout!"))
                )
              ) { r: VehicleReply => Future.successful(r) }
            )(system.executionContext)
          case Dropped => Future.failed(OverCapacity(reqId))
          case other   => Future.failed(Error(other))
        }

      case get: GetLocation =>
        val VehicleKey = ReplicatedVehicle.Key(get.vehicleId.toString)
        (stateReplicator ? Get(VehicleKey, readMajority)).map {
          case r @ GetSuccess(_, _) =>
            val repVehicle = r.get(VehicleKey)
            val status =
              if (repVehicle.version >= get.version) VehicleReply.ReplyStatusCode.Durable
              else VehicleReply.ReplyStatusCode.NotDurable
            VehicleReply(
              get.vehicleId,
              domain.types.protobuf.Location(repVehicle.state.lat, repVehicle.state.lon),
              status,
              VehicleReply.DurabilityLevel.Majority,
              com.rides.Versions(repVehicle.version, get.version)
            )

          case GetFailure(_, _) =>
            VehicleReply(
              get.vehicleId,
              get.local,
              VehicleReply.ReplyStatusCode.MajorityReadError,
              VehicleReply.DurabilityLevel.Majority,
              com.rides.Versions(-1, get.version)
            )

          case NotFound(_, _) =>
            VehicleReply(
              get.vehicleId,
              domain.types.protobuf.Location(),
              VehicleReply.ReplyStatusCode.NotFound,
              VehicleReply.DurabilityLevel.Majority,
              com.rides.Versions(-1, get.version)
            )
        }(system.executionContext)

      case StopEntity() =>
        throw new Exception(s"Unexpected ${classOf[StopEntity].getName} !")
    }

  private def shutdown(): Unit =
    queue.complete()

  private def whenDone: Future[Done] =
    done

  private def resume(cause: Throwable) = {
    logger.warn("Processor {} failed and resumes {}", name, cause)
    Supervision.Resume
  }
}
