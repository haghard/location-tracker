package akka.cluster.ddata.durable.raf

import akka.actor.typed.ActorSystem
import akka.cluster.ddata.DurableStore.DurableDataEnvelope
import akka.cluster.ddata.durable.raf.SharedMemoryLongMap.SharedMemoryValue
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import one.nio.serial.{CalcSizeStream, DataStream, JsonReader}

import java.lang

//shouldn't be renamed because it's stored in the raf file
final class RafSerializer(system: ActorSystem[_])
    extends one.nio.serial.Serializer[SharedMemoryValue](classOf[SharedMemoryValue]) {

  val serializer =
    SerializationExtension(system)
      .serializerFor(classOf[DurableDataEnvelope])
      .asInstanceOf[SerializerWithStringManifest]

  val manifest = serializer.manifest(new DurableDataEnvelope(akka.cluster.ddata.Replicator.Internal.DeletedData))

  override def calcSize(obj: SharedMemoryValue, css: CalcSizeStream): Unit = {
    val envBts = serializer.toBinary(obj.envelope)
    val digBts = obj.digest

    css.writeInt(envBts.length)
    css.write(envBts)

    // println(s"read: ${digBts.map(byte => f"$byte%02x").mkString("")}")
    css.writeInt(digBts.length)
    css.write(digBts)

    // 1u - 1072, 2u - 1104, 10u - 1361
    // println(s"length: ${envBts.length + digBts.length}")
  }

  override def write(obj: SharedMemoryValue, out: DataStream): Unit = {
    val dataEnvelopeBts = serializer.toBinary(obj.envelope)
    val digBts          = obj.digest
    // println(s"write: ${digBts.map(byte => f"$byte%02x").mkString("")}")

    out.writeInt(dataEnvelopeBts.length)
    out.write(dataEnvelopeBts)

    out.writeInt(digBts.length)
    out.write(digBts)
  }

  override def read(in: DataStream): SharedMemoryValue = {
    val valueBytes: Array[Byte] = Array.ofDim(in.readInt())
    in.read(valueBytes)

    val envelope = serializer.fromBinary(valueBytes, manifest) match {
      case e: akka.cluster.ddata.DurableStore.DurableDataEnvelope => e.dataEnvelope
      case _ =>
        throw new Exception(
          s"Read exception: Expected ${classOf[akka.cluster.ddata.DurableStore.DurableDataEnvelope].getName}"
        )
    }

    val digBytes: Array[Byte] = Array.ofDim(in.readInt())
    in.read(digBytes)

    new SharedMemoryValue(envelope, digBytes)
  }

  override def skip(in: DataStream): Unit                                        = {}
  override def toJson(obj: SharedMemoryValue, builder: lang.StringBuilder): Unit = {}

  override def fromJson(in: JsonReader): SharedMemoryValue = ???
}
