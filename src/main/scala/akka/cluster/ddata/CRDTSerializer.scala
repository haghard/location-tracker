package akka.cluster
package ddata

import akka.actor.{AddressFromURIString, ExtendedActorSystem}
import akka.cluster.ddata.crdt.protoc.{Dot, ReplicatedVehiclePB, ReplicatedVehicleRangePB}
import akka.cluster.ddata.protobuf.ReplicatedDataSerializer
import akka.cluster.ddata.replicator.{ReplicatedVehicle, ReplicatedVehicleRange}

final class CRDTSerializer(system: ExtendedActorSystem)
    extends ReplicatedDataSerializer(system)
    with akka.cluster.ddata.protobuf.SerializationSupport
    with ProtocDDataSupport {

  private val ReplicatedVManifest      = "A"
  private val ReplicatedVRangeManifest = "B"

  override def manifest(obj: AnyRef): String =
    obj match {
      case _: ReplicatedVehicle      => ReplicatedVManifest
      case _: ReplicatedVehicleRange => ReplicatedVRangeManifest
      case _                         => super.manifest(obj)
    }

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case reg: akka.cluster.ddata.LWWRegister[_] @unchecked => // akka.cluster.sharding.ShardCoordinator.Internal.State
        reg.value match {
          // State from akka.cluster.sharding.DDataShardCoordinator
          case state: akka.cluster.sharding.ShardCoordinator.Internal.State =>
            // if (ThreadLocalRandom.current().nextDouble() > 0.7)
            system.log.warning("Shards online: {} ", state.shards.keySet.size)
            // system.log.warning("********* Shards regions online: [{}] ", state.shards.values.map(_.path.toString).mkString(","))
            super.toBinary(obj)
          case vtype =>
            // system.log.warning("LWWRegister[{}]({}) ", vtype.getClass.getSimpleName, vtype)
            super.toBinary(obj)
        }

      case v: ReplicatedVehicle =>
        ReplicatedVehiclePB(
          // com.google.protobuf.UnsafeByteOperations.unsafeWrap(v.version.toByteArray),
          v.version,
          v.state,
          v.replicationState.toSeq.sortBy(_._1).map { case (uniqueAddress, version) =>
            Dot(
              uniqueAddress.address.toString,
              uniqueAddress.longUid,
              version
              // com.google.protobuf.UnsafeByteOperations.unsafeWrap(version.toByteArray)
            )
          }
        ).toByteArray

      /*case v: ReplicatedVehicleRange =>
        ReplicatedVehicleRangePB(
          // com.google.protobuf.UnsafeByteOperations.unsafeWrap(v.version.toByteArray),
          v.version,
          v.state,
          v.replicationState.toSeq.sortBy(_._1).map { case (uniqueAddress, version) =>
            Dot(
              uniqueAddress.address.toString,
              uniqueAddress.longUid,
              version
              // com.google.protobuf.UnsafeByteOperations.unsafeWrap(version.toByteArray)
            )
          }
        ).toByteArray*/

      // Other CRDT LWWHashMap(51 -> akka://rides@127.0.0.2:2550, 55 -> akka://rides@127.0.0.2:2550, 50 -> akka://rides@127.0.0.1:2550, 57 -> akka://rides@127.0.0.2:2550)
      case map: LWWMap[String, String] =>
        system.log.warning("Shard placement CRDT [{}]", map.entries.groupMap(_._2)(_._1).mkString(","))
        super.toBinary(obj)

      case crdt =>
        // system.log.warning("Other CRDT {}", crdt)
        super.toBinary(obj)
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case ReplicatedVManifest =>
        val pb = ReplicatedVehiclePB.parseFrom(bytes)
        ReplicatedVehicle(
          pb.vehicle,
          pb.version,
          // BigInt(pb.orderVer.toByteArray),
          pb.dots
            .map(entry => (UniqueAddress(AddressFromURIString(entry.address), entry.uid), entry.version))
            .toMap
        )

      case ReplicatedVRangeManifest =>
        val pb = ReplicatedVehicleRangePB.parseFrom(bytes)
        ReplicatedVehicleRange(
          pb.vehicles,
          pb.version,
          pb.dots
            .map(entry => (UniqueAddress(AddressFromURIString(entry.address), entry.uid), entry.version))
            .toMap
        )

      case _ =>
        super.fromBinary(bytes, manifest)
    }
}
