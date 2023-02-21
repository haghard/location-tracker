package akka.cluster.ddata

import akka.actor.ActorRef
import akka.actor.Address
import akka.cluster.Member
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Internal.*
import akka.cluster.ddata.protobuf.SerializationSupport
import akka.cluster.ddata.protobuf.msg.ReplicatorMessages as dm
import akka.cluster.sharding.ShardCoordinator.Internal.ShardHome
import akka.cluster.sharding.ShardCoordinator.Internal.ShardHomeAllocated
import akka.cluster.sharding.ShardCoordinator.Internal.ShardHomes
import akka.cluster.sharding.ShardCoordinator.Internal.State
import akka.remote.ByteStringUtils
import akka.serialization.Serialization
import akka.util.ByteString as AkkaByteString

import java.nio.ByteBuffer
import scala.collection.immutable
import scala.jdk.CollectionConverters.*

/** The methods were copied from `akka.cluster.ddata.protobuf.ReplicatorMessageSerializer`
  */
trait ProtocDDataSupport extends SerializationSupport {

  protected val KB = 1024

  private val dummyAddress = UniqueAddress(Address("a", "b", "c", 2552), 1L)

  def statusToProto(status: Status): dm.Status = {
    val b = dm.Status.newBuilder()
    b.setChunk(status.chunk).setTotChunks(status.totChunks)
    status.digests.foreach { case (key, digest) =>
      b.addEntries(
        dm.Status.Entry
          .newBuilder()
          .setKey(key)
          .setDigest(ByteStringUtils.toProtoByteStringUnsafe(digest.toArrayUnsafe()))
      )
    }
    status.toSystemUid.foreach(b.setToSystemUid) // can be None when sending back to a node of version 2.5.21
    b.setFromSystemUid(status.fromSystemUid.get)
    b.build()
  }

  /*
  def statusFromBinary(heapByteBuffer: ByteBuffer): Status = {
    val status        = dm.Status.parseFrom(heapByteBuffer)
    val toSystemUid   = if (status.hasToSystemUid) Some(status.getToSystemUid) else None
    val fromSystemUid = if (status.hasFromSystemUid) Some(status.getFromSystemUid) else None
    Status(
      status.getEntriesList.asScala.iterator
        .map(e ⇒ e.getKey → akka.util.ByteString(e.getDigest.toByteArray()))
        .toMap,
      status.getChunk,
      status.getTotChunks,
      toSystemUid,
      fromSystemUid
    )
  }*/

  def statusFromBinary(heapByteBuffer: ByteBuffer): Status = {
    val status        = dm.Status.parseFrom(heapByteBuffer)
    val toSystemUid   = if (status.hasToSystemUid) Some(status.getToSystemUid) else None
    val fromSystemUid = if (status.hasFromSystemUid) Some(status.getFromSystemUid) else None
    Status(
      status.getEntriesList.asScala.iterator
        .map(e => e.getKey -> AkkaByteString.fromArrayUnsafe(e.getDigest.toByteArray()))
        // .map(e => e.getKey -> akka.util.ByteString(e.getDigest.toByteArray()))
        .toMap,
      status.getChunk,
      status.getTotChunks,
      toSystemUid,
      fromSystemUid
    )
  }

  private def pruningFromProto(
    pruningEntries: java.util.List[dm.DataEnvelope.PruningEntry]
  ): Map[UniqueAddress, PruningState] =
    if (pruningEntries.isEmpty)
      Map.empty
    else
      pruningEntries.asScala.iterator.map { pruningEntry =>
        val state =
          if (pruningEntry.getPerformed) {
            // for wire compatibility with Akka 2.4.x
            val obsoleteTime = if (pruningEntry.hasObsoleteTime) pruningEntry.getObsoleteTime else Long.MaxValue
            PruningState.PruningPerformed(obsoleteTime)
          } else
            PruningState.PruningInitialized(
              uniqueAddressFromProto(pruningEntry.getOwnerAddress),
              pruningEntry.getSeenList.asScala.iterator.map(addressFromProto).to(scala.collection.immutable.Set)
            )
        val removed = uniqueAddressFromProto(pruningEntry.getRemovedAddress)
        removed -> state
      }.toMap

  def dataEnvelopeFromProto(dataEnvelope: dm.DataEnvelope): DataEnvelope = {
    val data    = otherMessageFromProto(dataEnvelope.getData).asInstanceOf[ReplicatedData]
    val pruning = pruningFromProto(dataEnvelope.getPruningList)
    val deltaVersions =
      if (dataEnvelope.hasDeltaVersions) versionVectorFromProto(dataEnvelope.getDeltaVersions)
      else VersionVector.empty
    DataEnvelope(data, pruning, deltaVersions)
  }

  def gossipFromBinary(byteBuffer: ByteBuffer): Gossip = {
    val gossip        = dm.Gossip.parseFrom(byteBuffer)
    val toSystemUid   = if (gossip.hasToSystemUid) Some(gossip.getToSystemUid) else None
    val fromSystemUid = if (gossip.hasFromSystemUid) Some(gossip.getFromSystemUid) else None
    Gossip(
      gossip.getEntriesList.asScala.iterator.map(e => e.getKey -> dataEnvelopeFromProto(e.getEnvelope)).toMap,
      sendBack = gossip.getSendBack,
      toSystemUid,
      fromSystemUid
    )
  }

  def gossipToProto(gossip: Gossip): dm.Gossip = {
    val b = dm.Gossip.newBuilder().setSendBack(gossip.sendBack)
    gossip.updatedData.foreach { case (key, data) =>
      b.addEntries(dm.Gossip.Entry.newBuilder().setKey(key).setEnvelope(dataEnvelopeToProto(data)))
    }
    gossip.toSystemUid.foreach(b.setToSystemUid)
    b.setFromSystemUid(gossip.fromSystemUid.get)
    b.build()
  }

  def pruningToProto(
    entries: Map[UniqueAddress, PruningState]
  ): Iterable[akka.cluster.ddata.protobuf.msg.ReplicatorMessages.DataEnvelope.PruningEntry] =
    entries.map { case (removedAddress, state) =>
      val b = akka.cluster.ddata.protobuf.msg.ReplicatorMessages.DataEnvelope.PruningEntry
        .newBuilder()
        .setRemovedAddress(uniqueAddressToProto(removedAddress))
      state match {
        case PruningState.PruningInitialized(owner, seen) =>
          seen.toVector.sorted(Member.addressOrdering).map(addressToProto).foreach { a =>
            b.addSeen(a)
          }
          b.setOwnerAddress(uniqueAddressToProto(owner))
          b.setPerformed(false)
        case PruningState.PruningPerformed(obsoleteTime) =>
          b.setPerformed(true).setObsoleteTime(obsoleteTime)
          // TODO ownerAddress is only needed for PruningInitialized, but kept here for
          // wire backwards compatibility with 2.4.16 (required field)
          b.setOwnerAddress(uniqueAddressToProto(dummyAddress))
      }
      b.build()
    }

  def dataEnvelopeToProto(dataEnvelope: DataEnvelope): dm.DataEnvelope = {
    val dataEnvelopeBuilder = dm.DataEnvelope.newBuilder().setData(otherMessageToProto(dataEnvelope.data))
    dataEnvelopeBuilder.addAllPruning(pruningToProto(dataEnvelope.pruning).asJava)

    if (!dataEnvelope.deltaVersions.isEmpty)
      dataEnvelopeBuilder.setDeltaVersions(versionVectorToProto(dataEnvelope.deltaVersions))

    dataEnvelopeBuilder.build()
  }

  def deltaPropagationFromBinary(bytes: ByteBuffer): DeltaPropagation = {

    def dataEnvelopeFromProto(dataEnvelope: dm.DataEnvelope): DataEnvelope = {
      val data    = otherMessageFromProto(dataEnvelope.getData).asInstanceOf[ReplicatedData]
      val pruning = pruningFromProto(dataEnvelope.getPruningList)
      val deltaVersions =
        if (dataEnvelope.hasDeltaVersions) versionVectorFromProto(dataEnvelope.getDeltaVersions)
        else VersionVector.empty
      DataEnvelope(data, pruning, deltaVersions)
    }

    val deltaPropagation = dm.DeltaPropagation.parseFrom(bytes)
    val reply            = deltaPropagation.hasReply && deltaPropagation.getReply
    DeltaPropagation(
      uniqueAddressFromProto(deltaPropagation.getFromNode),
      reply,
      deltaPropagation.getEntriesList.asScala.iterator.map { e =>
        val fromSeqNr = e.getFromSeqNr
        val toSeqNr   = if (e.hasToSeqNr) e.getToSeqNr else fromSeqNr
        e.getKey -> Delta(dataEnvelopeFromProto(e.getEnvelope), fromSeqNr, toSeqNr)
      }.toMap
    )
  }

  def readResultFromBinary(byteBuffer: ByteBuffer): ReadResult = {
    val readResult = dm.ReadResult.parseFrom(byteBuffer)
    val envelope =
      if (readResult.hasEnvelope) Some(dataEnvelopeFromProto(readResult.getEnvelope))
      else None
    ReadResult(envelope)
  }

  def readResultToProto(readResult: ReadResult): dm.ReadResult = {
    val b = dm.ReadResult.newBuilder()
    readResult.envelope match {
      case Some(d) => b.setEnvelope(dataEnvelopeToProto(d))
      case None    =>
    }
    b.build()
  }

  import akka.cluster.sharding.protobuf.msg.ClusterShardingMessages as sm
  def coordinatorStateFromProto(state: sm.CoordinatorState): State = {
    val shards: Map[String, ActorRef] =
      state.getShardsList.asScala.toVector.iterator.map { entry =>
        entry.getShardId -> resolveActorRef(entry.getRegionRef)
      }.toMap

    val regionsZero: Map[ActorRef, Vector[String]] =
      state.getRegionsList.asScala.toVector.iterator.map(resolveActorRef(_) -> Vector.empty[String]).toMap
    val regions: Map[ActorRef, Vector[String]] =
      shards.foldLeft(regionsZero) { case (acc, (shardId, regionRef)) =>
        acc.updated(regionRef, acc(regionRef) :+ shardId)
      }

    val proxies: Set[ActorRef] = state.getRegionProxiesList.asScala.iterator.map(resolveActorRef).to(immutable.Set)
    val unallocatedShards: Set[String] = state.getUnallocatedShardsList.asScala.toSet

    State(shards, regions, proxies, unallocatedShards)
  }

  def coordinatorStateToProto(state: State): sm.CoordinatorState = {
    val builder = sm.CoordinatorState.newBuilder()

    state.shards.foreach { case (shardId, regionRef) =>
      val b = sm.CoordinatorState.ShardEntry
        .newBuilder()
        .setShardId(shardId)
        .setRegionRef(Serialization.serializedActorPath(regionRef))
      builder.addShards(b)
    }
    state.regions.foreach { case (regionRef, _) =>
      builder.addRegions(Serialization.serializedActorPath(regionRef))
    }
    state.regionProxies.foreach { ref =>
      builder.addRegionProxies(Serialization.serializedActorPath(ref))
    }
    state.unallocatedShards.foreach(builder.addUnallocatedShards)

    builder.build()
  }

  def coordinatorStateFromBinary(bytes: Array[Byte]): State =
    coordinatorStateFromProto(sm.CoordinatorState.parseFrom(bytes))

  def coordinatorStateFromBinary(bts: ByteBuffer): State =
    coordinatorStateFromProto(sm.CoordinatorState.parseFrom(bts))

  def actorRefMessageFromBinaryBuffer(buf: ByteBuffer): ActorRef =
    resolveActorRef(sm.ActorRefMessage.parseFrom(buf).getRef)

  def actorRefMessageFromBinary(bytes: Array[Byte]): ActorRef =
    resolveActorRef(sm.ActorRefMessage.parseFrom(bytes).getRef)

  def actorRefMessageToProto(ref: ActorRef): sm.ActorRefMessage =
    sm.ActorRefMessage.newBuilder().setRef(Serialization.serializedActorPath(ref)).build()

  def shardIdMessageFromBinaryBuffer(bytes: ByteBuffer): String =
    sm.ShardIdMessage.parseFrom(bytes).getShard

  def shardIdMessageFromBinary(bytes: Array[Byte]): String =
    sm.ShardIdMessage.parseFrom(bytes).getShard

  def shardIdMessageToProto(shardId: String): sm.ShardIdMessage =
    sm.ShardIdMessage.newBuilder().setShard(shardId).build()

  def shardHomeToProto(m: ShardHome): sm.ShardHome =
    sm.ShardHome.newBuilder().setShard(m.shard).setRegion(Serialization.serializedActorPath(m.ref)).build()

  def shardHomesToProto(sh: ShardHomes): sm.ShardHomes =
    sm.ShardHomes
      .newBuilder()
      .addAllHomes(
        sh.homes.map { case (regionRef, shards) =>
          sm.ShardHomesEntry
            .newBuilder()
            .setRegion(Serialization.serializedActorPath(regionRef))
            .addAllShard(shards.asJava)
            .build()
        }.asJava
      )
      .build()

  def shardHomesFromBinary(buf: ByteBuffer): ShardHomes = {
    val sh = sm.ShardHomes.parseFrom(buf)
    ShardHomes(sh.getHomesList.asScala.map { she =>
      resolveActorRef(she.getRegion) -> she.getShardList.asScala.toVector
    }.toMap)
  }

  def shardHomeAllocatedFromBinary(buf: ByteBuffer): ShardHomeAllocated = {
    val m = sm.ShardHomeAllocated.parseFrom(buf)
    ShardHomeAllocated(m.getShard, resolveActorRef(m.getRegion))
  }

  def writeFromBinary(buf: ByteBuffer): Write = {
    val write    = dm.Write.parseFrom(buf)
    val env      = dataEnvelopeFromProto(write.getEnvelope)
    val fromNode = if (write.hasFromNode) Some(uniqueAddressFromProto(write.getFromNode)) else None
    Write(write.getKey, env, fromNode)
  }

  def writeToProto(write: Write): dm.Write =
    dm.Write
      .newBuilder()
      .setKey(write.key)
      .setEnvelope(dataEnvelopeToProto(write.envelope))
      .setFromNode(uniqueAddressToProto(write.fromNode.get))
      .build()

  def pruningStateToProto(pruningState: PruningState.PruningInitialized): dm.DataEnvelope.PruningEntry = {
    val b = dm.DataEnvelope.PruningEntry.newBuilder().setRemovedAddress(uniqueAddressToProto(dummyAddress))
    pruningState match {
      case PruningState.PruningInitialized(owner, seen) =>
        seen.toVector.sorted(Member.addressOrdering).map(addressToProto).foreach { a =>
          b.addSeen(a)
        }
        b.setOwnerAddress(uniqueAddressToProto(owner))
        b.setPerformed(false)
      /*
      case PruningState.PruningPerformed(obsoleteTime) =>
        b.setPerformed(true).setObsoleteTime(obsoleteTime)
        b.setOwnerAddress(uniqueAddressToProto(dummyAddress))
       */
    }
    b.build()
  }

  def pruningStateFromProto(
    pruningStateBts: Array[Byte]
  ): PruningState = {
    val pruningStatePB = dm.DataEnvelope.PruningEntry.parseFrom(pruningStateBts)
    PruningState.PruningInitialized(
      uniqueAddressFromProto(pruningStatePB.getOwnerAddress),
      pruningStatePB.getSeenList.asScala.iterator.map(addressFromProto).to(immutable.Set)
    )
  }

  /*def voteEnvelopeStateToProto(
    owner: UniqueAddress,
    seen: Set[Address]
  ): dm.DataEnvelope.PruningEntry = {
    val b = dm.DataEnvelope.PruningEntry.newBuilder().setRemovedAddress(uniqueAddressToProto(dummyAddress))
    seen.toVector.sorted(Member.addressOrdering).map(addressToProto).foreach { a =>
      b.addSeen(a)
    }
    b.setOwnerAddress(uniqueAddressToProto(owner))
    b.setPerformed(false)
    b.build()
  }*/

  /*def voteEnvelopeStateFromProto(bts: Array[Byte], cnt: Long): VoteEnvelope = {
    val pruningStatePB = dm.DataEnvelope.PruningEntry.parseFrom(bts)
    VoteEnvelope(
      uniqueAddressFromProto(pruningStatePB.getOwnerAddress),
      immutable.SortedSet
        .from(pruningStatePB.getSeenList.asScala.iterator.map(addressFromProto))(Member.addressOrdering),
      cnt
    )
  }*/
}
