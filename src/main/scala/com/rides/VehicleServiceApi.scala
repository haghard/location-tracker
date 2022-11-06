package com.rides

import akka.actor.typed.ActorSystem
import akka.cluster.typed.Cluster
import akka.cluster.ddata.replicator.DDataReplicator
import akka.stream.scaladsl.Source
import com.rides.domain.types.protobuf.Location
import com.rides.domain.{GetLocation, ReportLocation}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class VehicleServiceApi(
  sharedMemoryMap: akka.cluster.ddata.durable.raf.SharedMemoryLongMap,
  stateReplicator: akka.actor.ActorRef,
  cluster: Cluster,
  vehicleApi: VehicleApi
)(implicit val system: ActorSystem[_])
    extends com.rides.VehicleService {

  //
  override def subscribe(req: SubscribeRequest): Source[VehicleReply, akka.NotUsed] =
    /*
    val it  = Iterator.from(0, 0 + 100).map(each => Location(3.701101d + each.toDouble, -3.701101d))
    val src = Source.fromIterator(() => it).throttle(1, 2.second).runWith(Sink.asPublisher(fanout = false))
    Source.fromPublisher(src)
     */

    // TODO
    // BroadcastHub.sink[Location]
    Option(sharedMemoryMap.get(req.vehicleId)).map(DDataReplicator.readLocal) match {
      case Some(_) =>
        Source
          .tick(1.second, 3.second, ())
          .map { _ =>
            val localValue = DDataReplicator.readLocal(sharedMemoryMap.get(req.vehicleId))
            VehicleReply(
              req.vehicleId,
              Location(localValue.state.lat, localValue.state.lon),
              if (localValue.isDurable(cluster.state.members.map(_.uniqueAddress))) VehicleReply.ReplyStatusCode.Durable
              else VehicleReply.ReplyStatusCode.NotDurable,
              VehicleReply.DurabilityLevel.Local,
              com.rides.Versions(localValue.version, localValue.version)
            )
          }
          .mapMaterializedValue(_ => akka.NotUsed)

      case None =>
        Source.empty
    }

  override def postLocation(req: PutLocation): Future[VehicleReply] = {
    val reqId = wvlet.airframe.ulid.ULID.newULID.toString
    vehicleApi.askApi(
      reqId,
      ReportLocation(req.vehicleId, Location(req.lon, req.lat) /*, requestId = reqId*/ )
    )
  }

  override def getCoordinates(req: GetRequest): Future[VehicleReply] =
    Option(sharedMemoryMap.get(req.vehicleId)).map(DDataReplicator.readLocal) match {
      case Some(localValue) =>
        val requestId = wvlet.airframe.ulid.ULID.newULID.toString
        if (localValue.isDurable(cluster.state.members.map(_.uniqueAddress))) {
          system.log.warn("Local value is durable ({},{})", localValue.version, req.version)
          if (localValue.version >= req.version)
            Future.successful(
              VehicleReply(
                req.vehicleId,
                Location(localValue.state.lat, localValue.state.lon),
                VehicleReply.ReplyStatusCode.Durable,
                VehicleReply.DurabilityLevel.Local,
                com.rides.Versions(localValue.version, req.version)
              )
            )
          else {
            system.log.warn(s"Local value is durable but version mismatch ({},{})", localValue.version, req.version)
            vehicleApi.askApi(
              requestId,
              GetLocation(
                vehicleId = req.vehicleId,
                version = req.version,
                local = Location(localValue.state.lat, localValue.state.lon)
              )
            )
          }
        } else {
          system.log.warn(
            "Local value most likely is durable, but we need to wait for pruning before `isDurable` starts returning true"
          )
          vehicleApi.askApi(
            requestId,
            GetLocation(
              vehicleId = req.vehicleId,
              version = req.version,
              local = Location(localValue.state.lat, localValue.state.lon)
            )
          )
          /*Future.successful(
            VehicleReply(
              req.vehicleId,
              Location(localValue.state.lat, localValue.state.lon),
              VehicleReply.ReplyStatusCode.Unknown, // wait for pruning
              VehicleReply.DurabilityLevel.Local,
              com.rides.Versions(localValue.version, req.version)
            )
          )*/
        }
      case None =>
        Future.successful(
          VehicleReply(
            req.vehicleId,
            Location(),
            VehicleReply.ReplyStatusCode.NotFound,
            VehicleReply.DurabilityLevel.Local,
            com.rides.Versions(-1, req.version)
          )
        )
    }
}
