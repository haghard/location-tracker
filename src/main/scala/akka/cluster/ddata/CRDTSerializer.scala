package akka.cluster
package ddata

import akka.actor.{AddressFromURIString, ExtendedActorSystem}
import akka.cluster.ddata.crdt.protoc.{Dot, ReplicatedVehiclePB}
import akka.cluster.ddata.protobuf.ReplicatedDataSerializer
import akka.cluster.ddata.replicator.ReplicatedVehicle

final class CRDTSerializer(system: ExtendedActorSystem)
    extends ReplicatedDataSerializer(system)
    with akka.cluster.ddata.protobuf.SerializationSupport
    with ProtocDDataSupport {

  private val ReplicatedOrderManifest = "ACT"

  override def manifest(obj: AnyRef): String =
    obj match {
      case _: ReplicatedVehicle => ReplicatedOrderManifest
      case _                    => super.manifest(obj)
    }

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case reg: akka.cluster.ddata.LWWRegister[akka.cluster.sharding.ShardCoordinator.Internal.State] @unchecked =>
        reg.value match {
          // State from akka.cluster.sharding.DDataShardCoordinator
          case state: akka.cluster.sharding.ShardCoordinator.Internal.State =>
            // if (ThreadLocalRandom.current().nextDouble() > 0.7)
            system.log.warning("Shards online: {} ", state.shards.keySet.size)
          case _ => super.toBinary(obj)
        }
        super.toBinary(obj)

      case or: ORSet[String] @unchecked =>
        // system.log.warning("ORSet({})", or.elements.mkString(","))
        super.toBinary(or)

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

      case crdt =>
        system.log.warning("Other CRDT {}", crdt)
        super.toBinary(obj)
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case ReplicatedOrderManifest =>
        val pb = ReplicatedVehiclePB.parseFrom(bytes)
        ReplicatedVehicle(
          pb.vehicle,
          pb.version,
          // BigInt(pb.orderVer.toByteArray),
          pb.dots
            .map(entry => (UniqueAddress(AddressFromURIString(entry.address), entry.uid), entry.version))
            .toMap
        )

      case _ =>
        super.fromBinary(bytes, manifest)
    }
}
