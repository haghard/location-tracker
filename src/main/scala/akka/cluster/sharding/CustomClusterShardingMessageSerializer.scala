package akka.cluster.sharding

import akka.actor.ExtendedActorSystem
import akka.cluster.ddata.ProtocDDataSupport
import akka.cluster.ddata.protobuf.SerializationSupport
import akka.cluster.sharding.ShardCoordinator.Internal
import akka.cluster.sharding.ShardCoordinator.Internal.*
import akka.cluster.sharding.protobuf.ClusterShardingMessageSerializer
import akka.protobufv3.internal.CodedOutputStream
import akka.serialization.ByteBufferSerializer

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
import scala.util.Using
import scala.util.Using.Releasable

import CustomClusterShardingMessageSerializer._

/*

com/typesafe/akka/akka-cluster-sharding_2.13/2.6.15/akka-cluster-sharding_2.13-2.6.15.jar!/reference.conf

serializers {
    akka-sharding = "akka.cluster.sharding.protobuf.ClusterShardingMessageSerializer"
  }
  serialization-bindings {
    "akka.cluster.sharding.ClusterShardingSerializable" = akka-sharding
  }
  serialization-identifiers {
    "akka.cluster.sharding.protobuf.ClusterShardingMessageSerializer" = 13
  }
 */

object CustomClusterShardingMessageSerializer {
  implicit val releasable: Releasable[CodedOutputStream] = _.flush()
}

//TODO: take a look at akka.cluster.sharding.typed.internal.ShardingSerializer
final class CustomClusterShardingMessageSerializer(system: ExtendedActorSystem)
    extends ClusterShardingMessageSerializer(system)
    with SerializationSupport
    with ByteBufferSerializer
    with ProtocDDataSupport {

  val CoordinatorStateManifest   = "AA"
  val RegisterAckManifest        = "BC"
  val GetShardHomeManifest       = "BD"
  val HostShardManifest          = "BF"
  val BeginHandOffManifest       = "BH"
  val ShardHomeAllocatedManifest = "AF" // akka.cluster.sharding.ShardCoordinator.Internal.ShardHomeAllocated
  val RegionStoppedManifest      = "BM"
  val ShardHomesManifest         = "BN"

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case state: akka.cluster.sharding.ShardCoordinator.Internal.State =>
        val bts = coordinatorStateToProto(state).toByteArray
        // if (ThreadLocalRandom.current().nextDouble() > 0.9)
        system.log.warning("a.c.s.ShardCoordinator.Internal.State: {} bts", bts.size)

        bts

      case _ =>
        super.toBinary(obj)
    }

  /*val ev: akka.cluster.sharding.ShardCoordinator.Internal.DomainEvent
  ev match {
    case ShardRegionRegistered(region) => ???
    case ShardRegionProxyRegistered(regionProxy) => ???
    case ShardRegionTerminated(region) => ???
    case ShardRegionProxyTerminated(regionProxy) => ???
    case ShardHomeAllocated(shard, region) => ??? //ShardHomeAllocatedManifest
    case ShardHomeDeallocated(shard) => ???
    case Internal.ShardCoordinatorInitialized => ???
  }

  This ADT updates akka.cluster.sharding.ShardCoordinator.Internal.State
   */

  override def toBinary(obj: AnyRef, directBuf: ByteBuffer): Unit = {
    val out = CodedOutputStream.newInstance(directBuf)
    obj match {
      case cm: CoordinatorMessage =>
        cm match {
          case RegisterAck(coordinator) =>
            Using.resource(out)(actorRefMessageToProto(coordinator).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(actorRefMessageToProto(coordinator).writeTo(_))
          case m: ShardHome =>
            Using.resource(out)(shardHomeToProto(m).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardHomeToProto(m).writeTo(_))
          case m: ShardHomes =>
            Using.resource(out)(shardHomesToProto(m).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardHomesToProto(m).writeTo(_))
          case HostShard(shard) =>
            Using.resource(out)(shardIdMessageToProto(shard).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardIdMessageToProto(shard).writeTo(_))
          case ShardStarted(shard) =>
            Using.resource(out)(shardIdMessageToProto(shard).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardIdMessageToProto(shard).writeTo(_))
          case BeginHandOff(shard) =>
            Using.resource(out)(shardIdMessageToProto(shard).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardIdMessageToProto(shard).writeTo(_))
          case HandOff(shard) =>
            Using.resource(out)(shardIdMessageToProto(shard).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardIdMessageToProto(shard).writeTo(_))
        }
      case cm: CoordinatorCommand =>
        cm match {
          case Register(shardRegion) =>
            Using.resource(out)(actorRefMessageToProto(shardRegion).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(actorRefMessageToProto(shardRegion).writeTo(_))
          case RegisterProxy(shardRegionProxy) =>
            Using.resource(out)(actorRefMessageToProto(shardRegionProxy).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(actorRefMessageToProto(shardRegionProxy).writeTo(_))
          case GetShardHome(shard) =>
            Using.resource(out)(shardIdMessageToProto(shard).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardIdMessageToProto(shard).writeTo(_))
          case BeginHandOffAck(shard) =>
            Using.resource(out)(shardIdMessageToProto(shard).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardIdMessageToProto(shard).writeTo(_))
          case ShardStopped(shard) =>
            Using.resource(out)(shardIdMessageToProto(shard).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(shardIdMessageToProto(shard).writeTo(_))
          case RegionStopped(shardRegion) =>
            Using.resource(out)(actorRefMessageToProto(shardRegion).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(actorRefMessageToProto(shardRegion).writeTo(_))
          case GracefulShutdownReq(shardRegion) =>
            // TODO: follow this pattern
            Using.resource(out)(actorRefMessageToProto(shardRegion).writeTo(_))
          // Using.resource(new ByteBufferOutputStream(directBuf))(actorRefMessageToProto(shardRegion).writeTo(_))
        }

      case state: akka.cluster.sharding.ShardCoordinator.Internal.State =>
        /** On-line shards
          *
          * Size: 1 shard takes 195 bts, 2 shards takes 263 bts, 3 shards takes 333 bts
          *
          * ~ 70 bytes per shard.
          *
          * 100 - 7_000 bts - 7 KB 1000 - 70_000 bts - 70 KB 2000 - 140_000 bts - 140 KB 4000 - 280_000 bts - 280 KB
          * 8000 - 560_000 bts - 560 KB 12000 - 1120_000 bts - 1 Mb
          */
        Using.resource(CodedOutputStream.newInstance(directBuf)) { cos =>
          val cState = coordinatorStateToProto(state)
          system.log.warning("toBinary.ShardCoordinatorState_1: {} bts", cState.getSerializedSize)
          cState.writeTo(cos)
        }
      // Using.resource(new ByteBufferOutputStream(directBuf))(cState.writeTo(_))

      // TODO: Gives some useful insights
      case ev: DomainEvent =>
        ev match {
          case ShardRegionRegistered(region) =>
            system.log.warning("ShardRegionRegistered {}", region)
          case ShardRegionProxyRegistered(regionProxy) =>
          case ShardRegionTerminated(region) =>
            system.log.warning("ShardRegionTerminated {}", region)
          case ShardRegionProxyTerminated(regionProxy) =>
          case ShardHomeAllocated(shard, region) =>
            system.log.warning("ShardHomeAllocated {} on {}", shard, region)
          case ShardHomeDeallocated(shard) =>
            system.log.warning("ShardHomeDeallocated {}", shard)
          case Internal.ShardCoordinatorInitialized =>
            system.log.warning("ShardCoordinatorInitialized")
        }

        // TODO: implement all
        val array = super.toBinary(obj)
        directBuf.put(array)

      case _ =>
        // TODO: implement all
        // system.log.warning("!!! toBinary: {}", obj.getClass.getName)
        val array = super.toBinary(obj)
        directBuf.put(array)
    }
  }

  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef =
    manifest match {
      case RegisterAckManifest =>
        RegisterAck(actorRefMessageFromBinaryBuffer(buf))
      case GetShardHomeManifest =>
        GetShardHome(shardIdMessageFromBinaryBuffer(buf))
      case HostShardManifest =>
        HostShard(shardIdMessageFromBinaryBuffer(buf))
      case BeginHandOffManifest =>
        BeginHandOff(shardIdMessageFromBinaryBuffer(buf))
      case CoordinatorStateManifest =>
        coordinatorStateFromBinary(buf)
      case RegionStoppedManifest =>
        RegionStopped(actorRefMessageFromBinaryBuffer(buf))
      case ShardHomesManifest =>
        shardHomesFromBinary(buf)
      case _ =>
        // TODO: implement all
        // system.log.warning(s"add fromBinary for {}", manifest)
        // extra array
        val bytes = new Array[Byte](buf.remaining)
        buf.get(bytes)
        super.fromBinary(bytes, manifest)
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case CoordinatorStateManifest =>
        if (ThreadLocalRandom.current().nextDouble() > 0.9)
          system.log.warning("fromBinary.ShardCoordinatorState: {} bts", bytes.size)
        coordinatorStateFromBinary(bytes)

      case _ =>
        super.fromBinary(bytes, manifest)
    }
}
