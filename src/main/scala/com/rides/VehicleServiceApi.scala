package com.rides

import akka.actor.typed.ActorSystem
import akka.cluster.ddata.replicator.DDataReplicatorRocksDB
import akka.cluster.typed.Cluster
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.stream.scaladsl.Source
import com.google.common.primitives.Longs
import com.rides.domain.GetLocation
import com.rides.domain.ReportLocation
import com.rides.domain.types.protobuf.Location
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.RocksDB

import scala.concurrent.Future
import scala.util.Try

class VehicleServiceApi(
  // sharedMemoryMap: akka.cluster.ddata.durable.raf.SharedMemoryLongMap,
  db: RocksDB,
  columnFamily: ColumnFamilyHandle,
  cluster: Cluster,
  vehicleApi: VehicleApi
)(implicit val system: ActorSystem[_])
    extends com.rides.VehicleService {

  val serializer = SerializationExtension(system)
    .serializerOf("akka.cluster.ddata.CustomReplicatorMessageSerializerUdp")
    .get
    .asInstanceOf[SerializerWithStringManifest]

  override def subscribe(req: SubscribeRequest): Source[VehicleReply, akka.NotUsed] =
    Source.empty

  /*
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
        // https://github.com/sebastian-alfers/akka-grpc-rich-error
        /*
        import com.google.protobuf.any.Any
        import com.google.protobuf.any.Any.toJavaProto
        import com.google.rpc.{Code, Status}
        import io.grpc.protobuf.StatusProto
        val status: Status = Status
          .newBuilder()
          .setCode(Code.INVALID_ARGUMENT.getNumber)
          .setMessage("What is wrong?")
          .addDetails(toJavaProto(Any.pack(new HelloErrorReply(errorMessage = "The password!"))))
          .build()
        Future.failed(StatusProto.toStatusRuntimeException(status))
   */

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
    }*/

  override def postLocation(req: PutLocation): Future[VehicleReply] = {
    val reqId = wvlet.airframe.ulid.ULID.newULID.toString
    vehicleApi.askApi(
      reqId,
      ReportLocation(req.vehicleId, Location(req.lon, req.lat) /*, requestId = reqId*/ )
    )
  }

  override def getCoordinates(req: GetRequest): Future[VehicleReply] =
    Try {
      val envelopeBts = db.get(columnFamily, Longs.toByteArray(req.vehicleId))

      // Before serving reads we need to check
      // 1. If the KV is durable.
      // 2. If yes, we compare localValue.version with req.version
      val localValue = DDataReplicatorRocksDB.readLocal(envelopeBts, serializer)
      val requestId  = wvlet.airframe.ulid.ULID.newULID.toString
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
          system.log.warn(
            s"LocalValue v({}) is durable but higher version v({}) requested",
            localValue.version,
            req.version
          )
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
          "LocalValue({}) most likely is durable, but we need to wait for pruning/gossip before `isDurable` starts returning true",
          localValue
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
            VehicleReply.DurabilityLevel.Majority,
            com.rides.Versions(localValue.version, req.version)
          )
        )*/
      }
    }.getOrElse(
      Future.successful(
        VehicleReply(
          req.vehicleId,
          Location(),
          VehicleReply.ReplyStatusCode.NotFound,
          VehicleReply.DurabilityLevel.Local,
          com.rides.Versions(-1, req.version)
        )
      )
    )

  /*
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
    }*/
}
