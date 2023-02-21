package akka.cluster.ddata

import akka.actor.ExtendedActorSystem
import akka.cluster.ddata.protobuf.ReplicatorMessageSerializer
import akka.cluster.ddata.protobuf.SerializationSupport
import akka.protobufv3.internal.CodedOutputStream
import akka.serialization.ByteBufferSerializer

import java.nio.ByteBuffer
import scala.util.Using
import scala.util.Using.Releasable

/** https://doc.akka.io/docs/akka/current/remoting-artery.html#bytebuffer-based-serialization
  *
  * Artery introduced a new serialization mechanism. This implementation takes advantage of new Artery serialization
  * mechanism which allows the ByteBufferSerializer to directly write to a shared java.nio.ByteBuffer instead of being
  * forced to allocate and return an Array[Byte] for each serialized message.
  *
  * This mechanism is implemented only for a subset of messages Internal.Gossip, Internal.Status, Internal.ReadResult,
  * Internal.WriteAck
  */
final class CustomReplicatorMessageSerializerUdp(system: ExtendedActorSystem)
    extends ReplicatorMessageSerializer(system)
    with SerializationSupport
    with ByteBufferSerializer
    with ProtocDDataSupport {

  implicit val releasable: Releasable[CodedOutputStream] = _.flush()

  // 12
  // override val identifier: Int = super.identifier

  private val writeAck = akka.cluster.ddata.protobuf.msg.ReplicatorMessages.Empty.getDefaultInstance
  private val empty    = writeAck.toByteArray

  // Artery introduces a new serialization mechanism which allows the ByteBufferSerializer to directly write into a shared java.nio.ByteBuffer
  // instead of being forced to allocate and return an Array[Byte] for each serialized message.
  override def toBinary(replicatorMsg: AnyRef, directByteBuffer: ByteBuffer): Unit =
    replicatorMsg match {
      case g: akka.cluster.ddata.Replicator.Internal.Gossip =>
        val protoGossip = gossipToProto(g)
        val gossipKeys  = g.updatedData.keySet
        system.log.warning(
          s"ToBinary.Gossip:(${protoGossip.getSerializedSize})bts. Keys:[${gossipKeys.take(3).mkString(",")} ...] Size:${gossipKeys.size}" // Size:${allKeys.size}
        )
        Using.resource(CodedOutputStream.newInstance(directByteBuffer))(protoGossip.writeTo(_))

      case s: akka.cluster.ddata.Replicator.Internal.Status =>
        val protoStatus = statusToProto(s)
        system.log.warning(
          // "ToBinary: Status size {}. EntriesCount: {}. Direct: {}",
          "ToBinary: Status [Size - {} KB. NumOfKeys - {}]",
          protoStatus.getSerializedSize / KB,
          protoStatus.getEntriesList.size()
          // protoStatus.getEntriesList.asScala.map(_.getKey).mkString(","),
          // protoStatus.getEntriesCount,
          // directByteBuffer.isDirect
        )
        // Using.resource(new ByteBufferOutputStream(directByteBuffer))(protoStatus.writeTo(_))
        Using.resource(CodedOutputStream.newInstance(directByteBuffer))(protoStatus.writeTo(_))

      case rr: akka.cluster.ddata.Replicator.Internal.ReadResult =>
        val readResultProto = readResultToProto(rr)
        // Using.resource(new ByteBufferOutputStream(directByteBuffer))(readResultProto.writeTo(_))
        Using.resource(CodedOutputStream.newInstance(directByteBuffer))(readResultProto.writeTo(_))

      case w: akka.cluster.ddata.Replicator.Internal.Write =>
        val wp = writeToProto(w)
        Using.resource(CodedOutputStream.newInstance(directByteBuffer))(wp.writeTo(_))

      case akka.cluster.ddata.Replicator.Internal.WriteAck =>
        // Using.resource(new ByteBufferOutputStream(directByteBuffer))(out ⇒ writeAck.writeTo(out))
        directByteBuffer.get(empty)

      case akka.cluster.ddata.Replicator.Internal.WriteNack =>
        directByteBuffer.get(empty)

      case akka.cluster.ddata.Replicator.Internal.DeltaNack =>
        directByteBuffer.get(empty)

      case _ =>
        // java.lang.IllegalArgumentException: Can't serialize object of type class akka.cluster.ddata.Replicator$Internal$DeltaNack$ in [akka.cluster.ddata.CustomReplicatorMessageSerializerUdp
        throw new IllegalArgumentException(
          s"Can't serialize object of type ${replicatorMsg.getClass} in [${getClass.getName}]"
        )
    }

  override def fromBinary(directByteBuffer: ByteBuffer, manifest: String): AnyRef =
    /*
    val direct = directByteBuffer.isDirect
    val size   = directByteBuffer.remaining
    system.log.warning("***  FromBinary: {}. Size: {}. Direct: {}", manifest, size, direct)
     */

    manifest match {
      case GossipManifest =>
        val gossip = gossipFromBinary(directByteBuffer)
        // if (ThreadLocalRandom.current().nextDouble() < .2)
        system.log.warning(
          s"FromBinary.Gossip(${directByteBuffer.remaining})bts. Keys:[${gossip.updatedData.keySet.size}]"
        )
        gossip
      case StatusManifest =>
        statusFromBinary(directByteBuffer)

      case DeltaPropagationManifest =>
        deltaPropagationFromBinary(directByteBuffer)

      case ReadResultManifest =>
        readResultFromBinary(directByteBuffer)

      case WriteAckManifest =>
        akka.cluster.ddata.Replicator.Internal.WriteAck

      case WriteManifest =>
        writeFromBinary(directByteBuffer)

      case _ =>
        // extra array
        val bytes = new Array[Byte](directByteBuffer.remaining)
        directByteBuffer.get(bytes)
        super.fromBinary(bytes, manifest)
    }
}
