package com.rides.persistence

import akka.Done
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter.{ClassicActorSystemOps, TypedActorRefOps}
import akka.actor.{ActorRef, ExtendedActorSystem, RootActorPath}
import akka.cluster.ddata.Replicator.*
import akka.cluster.ddata.replicator.ReplicatedVehicleRange
import akka.pattern.ask
import akka.persistence.state.DurableStateStoreProvider
import akka.persistence.state.scaladsl.GetObjectResult
import com.rides.VehicleReply
import com.rides.state.VehicleRange

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

final class ReplicatedVehicleRangeStateStore(system: ExtendedActorSystem) extends DurableStateStoreProvider {

  val stateReplicator = {
    val to = 5.seconds
    // akka://order-management/user/replicator
    val DataReplicatorPath = RootActorPath(system.deadLetters.path.address) / "user" / "replicator"
    Await.result(system.actorSelection(DataReplicatorPath).resolveOne(to), to)
  }

  override def scaladslDurableStateStore(): akka.persistence.state.scaladsl.DurableStateStore[Any] =
    new akka.persistence.state.scaladsl.DurableStateUpdateStore[VehicleRange.State]() {

      val refResolver = ActorRefResolver(system.toTyped)
      val replicaId   = akka.cluster.Cluster(system).selfMember.uniqueAddress

      implicit val ec          = system.dispatcher
      implicit val readTimeout = akka.util.Timeout(2.seconds)
      val readCL               = ReadMajority(readTimeout.duration)

      override def upsertObject(
        vehicleRangeId: String,
        revision: Long,
        vehicles: VehicleRange.State,
        tag: String
      ): Future[Done] = {
        val vehicleId = vehicles.updatedVehicleId
        val vehicle   = vehicles.vehicles(vehicleId)
        val location  = com.rides.domain.types.protobuf.Location(vehicle.lat, vehicle.lon)
        val respondee = refResolver.resolveActorRef[Any](vehicles.replyTo)

        // TODO: PUT a queue here ????
        // println("Req from !!!" + respondee.path.address)
        println("Req from !!!" + respondee.path)
        // println("***************************" + respondee.path.address.hasLocalScope)

        if (respondee.path.address.hasLocalScope) {
          stateReplicator.tell(
            Update(
              ReplicatedVehicleRange.Key(vehicleRangeId),
              ReplicatedVehicleRange(vehicles),
              WriteLocal,
              Some((vehicleId, revision, location))
            )(_.update(vehicles.withReplyTo(""), replicaId, revision)),
            respondee.toClassic
          )
          Future.successful(akka.Done)
        } else {
          // TODO: not sure if I need this
          val p = Promise[akka.Done]
          stateReplicator.tell(
            Update(
              ReplicatedVehicleRange.Key(vehicleRangeId),
              ReplicatedVehicleRange(vehicles),
              WriteLocal,
              Some((vehicleId, revision, location))
            ) { ddata =>
              val updated = ddata.update(vehicles.withReplyTo(""), replicaId, revision)
              p.trySuccess(akka.Done)
              updated
            },
            respondee.toClassic
          )
          p.future
        }

        /*
        if (respondee.path.address.hasLocalScope) {
          // Local operation
          stateReplicator.tell(
            Update(
              ReplicatedVehicleRange.Key(vehicleRangeId),
              ReplicatedVehicleRange(vehicles),
              WriteLocal,
              Some((vehicleId, revision, location))
            )(_.update(vehicles.withReplyTo("").withRequestId(""), replicaId, revision)),
            respondee.toClassic
          )

          Future.successful(akka.Done)
        } else {
          (stateReplicator ? Update(
            ReplicatedVehicleRange.Key(vehicleRangeId),
            ReplicatedVehicleRange(vehicles),
            WriteLocal
          )(
            _.update(vehicles.withReplyTo("").withRequestId(""), replicaId, revision)
          )).map(
            onResponse(
              _,
              respondee,
              VehicleReply(
                vehicleId,
                location,
                com.rides.VehicleReply.ReplyStatusCode.MasterAcknowledged,
                com.rides.VehicleReply.DurabilityLevel.Local,
                com.rides.Versions(revision, revision)
              )
            )
          )
        }*/

      }

      override def getObject(persistenceId: String): Future[GetObjectResult[VehicleRange.State]] =
        attemptGet(stateReplicator, persistenceId, readCL)

      private def attemptGet(
        replicator: ActorRef,
        persistenceId: String,
        rc: ReadConsistency
      ): Future[GetObjectResult[VehicleRange.State]] = {
        val Key = ReplicatedVehicleRange.Key(persistenceId)
        (replicator ? Get(Key, rc))(readTimeout).flatMap {
          case r @ GetSuccess(_, _) =>
            val replicatedVehicleRange = r.get(Key)
            Future.successful(GetObjectResult(Some(replicatedVehicleRange.state), replicatedVehicleRange.version))
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
