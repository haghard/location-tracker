package com.rides.persistence

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest
import com.rides.VehicleReply
import com.rides.domain.{GetLocation, ReportLocation}
import com.rides.domain.types.protobuf.VehicleStatePB

import java.io.NotSerializableException

class VehicleStateSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest {

  override val identifier: Int = 999

  override def manifest(o: AnyRef): String = o.getClass.getSimpleName

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case p: VehicleStatePB => p.toByteArray
    case p: VehicleReply   => p.toByteArray
    case p: ReportLocation => p.toByteArray
    case p: GetLocation    => p.toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Unable to serialize to bytes, class was: ${obj.getClass}!")
  }

  override def fromBinary(
    bytes: Array[Byte],
    manifest: String
  ): AnyRef =
    if (manifest == classOf[VehicleStatePB].getSimpleName)
      VehicleStatePB.parseFrom(bytes)
    else if (manifest == classOf[VehicleReply].getSimpleName) VehicleReply.parseFrom(bytes)
    else if (manifest == classOf[ReportLocation].getSimpleName) ReportLocation.parseFrom(bytes)
    else if (manifest == classOf[GetLocation].getSimpleName) GetLocation.parseFrom(bytes)
    else
      throw new NotSerializableException(
        s"Unable to deserialize from bytes, manifest was: $manifest! Bytes length: ${bytes.length}"
      )
}
