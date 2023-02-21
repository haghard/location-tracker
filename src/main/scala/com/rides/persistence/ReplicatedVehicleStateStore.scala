package com.rides.persistence

import akka.Done
import akka.actor.ActorRef
import akka.actor.ExtendedActorSystem
import akka.actor.RootActorPath
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.cluster.ddata.Replicator.*
import akka.cluster.ddata.replicator.ReplicatedVehicle
import akka.pattern.ask
import akka.persistence.state.DurableStateStoreProvider
import akka.persistence.state.scaladsl.GetObjectResult
import com.rides.VehicleReply
import com.rides.state.Vehicle

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

final class ReplicatedVehicleStateStore(system: ExtendedActorSystem) extends DurableStateStoreProvider {

  val stateReplicator = {
    val to = 5.seconds // TODO:
    // akka://location-tracker/user/replicator
    val DataReplicatorPath = RootActorPath(system.deadLetters.path.address) / "user" / "replicator"
    Await.result(system.actorSelection(DataReplicatorPath).resolveOne(to), to)
  }

  override def scaladslDurableStateStore(): akka.persistence.state.scaladsl.DurableStateStore[Any] =
    new akka.persistence.state.scaladsl.DurableStateUpdateStore[Vehicle.State]() {

      val refResolver = ActorRefResolver(system.toTyped)
      val replicaId   = akka.cluster.Cluster(system).selfMember.uniqueAddress

      implicit val readTimeout = akka.util.Timeout(3.seconds)
      implicit val ec          = system.dispatcher

      val readCL = ReadMajority(readTimeout.duration)

      override def upsertObject(
        vehicleId: String,
        revision: Long,
        vehicle: Vehicle.State,
        tag: String
      ): Future[Done] = {
        // val replyTo = refResolver.resolveActorRef[VehicleReply](vehicle.replyTo)
        val respondee = refResolver.resolveActorRef[Any](vehicle.replyTo)
        val location  = com.rides.domain.types.protobuf.Location(vehicle.lat, vehicle.lon)

        if (respondee.path.address.hasLocalScope) {
          stateReplicator.tell(
            Update(
              ReplicatedVehicle.Key(vehicleId),
              ReplicatedVehicle(vehicle.withVehicleId(vehicleId.toLong)),
              WriteLocal,
              Some((vehicleId.toLong, revision, location))
            )(_.update(vehicle.withReplyTo(""), replicaId, revision)),
            respondee.toClassic
          )
          Future.successful(akka.Done)
        } else {
          // TODO: not sure if I need this

          // mimics the remote Promise
          val p = Promise[akka.Done]
          stateReplicator.tell(
            Update(
              ReplicatedVehicle.Key(vehicleId),
              ReplicatedVehicle(vehicle.withVehicleId(vehicleId.toLong)),
              WriteLocal,
              Some((vehicleId.toLong, revision, location))
            ) { ddata =>
              val updated = ddata.update(vehicle.withReplyTo(""), replicaId, revision)
              p.trySuccess(akka.Done)
              updated
            },
            respondee.toClassic
          )
          p.future
        }

        /*(stateReplicator ? Update(
          ReplicatedVehicle.Key(vehicleId),
          ReplicatedVehicle(vehicle.withVehicleId(vehicleId.toLong)),
          WriteLocal
        )(
          _.update(vehicle.withReplyTo(""), replicaId, revision)
        )).map(
          onResponse(
            _,
            replyTo,
            VehicleReply(
              vehicleId.toLong,
              com.rides.domain.types.protobuf.Location(vehicle.lat, vehicle.lon),
              com.rides.VehicleReply.ReplyStatusCode.MasterAcknowledged,
              com.rides.VehicleReply.DurabilityLevel.Local,
              com.rides.Versions(revision, revision)
            )
          )
        )*/
      }

      override def getObject(persistenceId: String): Future[GetObjectResult[Vehicle.State]] =
        attemptGet(stateReplicator, persistenceId, readCL)

      private def attemptGet(
        replicator: ActorRef,
        persistenceId: String,
        rc: ReadConsistency
      ): Future[GetObjectResult[Vehicle.State]] = {
        val Key = ReplicatedVehicle.Key(persistenceId)
        (replicator ? Get(Key, rc))(readTimeout).flatMap {
          case r @ GetSuccess(_, _) =>
            val replicatedVehicle = r.get(Key)
            Future.successful(GetObjectResult(Some(replicatedVehicle.state), replicatedVehicle.version))
          case NotFound(_, _) =>
            Future.successful(GetObjectResult(None, 0))
          case GetFailure(_, _) =>
            attemptGet(replicator, persistenceId, ReadLocal)
        }
      }

      private def onResponse(
        resp: Any,
        replyTo: akka.actor.typed.ActorRef[VehicleReply],
        reply: VehicleReply
      ): akka.Done =
        resp match {
          case UpdateSuccess(_, _) =>
            replyTo.tell(reply)
            akka.Done
          case UpdateTimeout(_, _) =>
            replyTo.tell(reply)
            // A timeout, just assume it will be eventually replicated
            akka.Done
          case e: UpdateFailure[_] =>
            throw new IllegalStateException("Unexpected failure: " + e)
        }

      override def deleteObject(persistenceId: String): Future[Done] =
        Future.failed(new Exception(s"Deletion $persistenceId"))

      override def deleteObject(persistenceId: String, revision: Long): Future[Done] =
        Future.failed(new Exception(s"Deletion $persistenceId:$revision"))

    }.asInstanceOf[akka.persistence.state.scaladsl.DurableStateStore[Any]]

  override def javadslDurableStateStore(): akka.persistence.state.javadsl.DurableStateStore[AnyRef] = null
}
