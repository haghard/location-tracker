package akka.cluster.ddata.replicator

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.Props
import akka.actor.Stash
import akka.actor.Timers
import akka.cluster.ClusterEvent.*
import akka.cluster.*
import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Key.KeyR
import akka.cluster.ddata.*
import akka.cluster.ddata.durable.raf.SharedMemoryLongMap.SharedMemoryValue
import akka.event.Logging
import akka.remote.RARP
import akka.serialization.SerializationExtension
import akka.util.ByteString
import one.nio.async.AsyncExecutor

import java.io.RandomAccessFile
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using
import scala.util.control.NonFatal

import DDataReplicator2._

object DDataReplicator2 {

  val NotFoundDig = Array[Byte](-1)
  val DeletedDig  = Array.emptyByteArray

  sealed trait DigestStatus

  object DigestStatus {
    case object Different extends DigestStatus

    case object Same extends DigestStatus

    case object NotFound extends DigestStatus
  }

  def vehicleId2Long(vehicleId: String): Long = java.lang.Long.parseLong(vehicleId)

  def backup(srcFilePath: String) =
    Using.resource(new RandomAccessFile(srcFilePath, "rw").getChannel) { in =>
      Using.resource(new RandomAccessFile(srcFilePath + "_" + System.currentTimeMillis(), "rw").getChannel) { out =>
        val size     = in.size()
        var position = 0L
        while (position < size)
          position += in.transferTo(position, 50 * (1024L * 1024L), out)
      }
    }

  def readLocal(
    value: akka.cluster.ddata.durable.raf.SharedMemoryLongMap.SharedMemoryValue
  ): ReplicatedVehicleRange =
    value.envelope match {
      case ev: akka.cluster.ddata.Replicator.Internal.DataEnvelope =>
        ev.data.asInstanceOf[ReplicatedVehicleRange]
      case _ =>
        throw new Exception("readLocal failure")
    }

  def props(
    sharedMemoryMap: akka.cluster.ddata.durable.raf.SharedMemoryLongMap,
    settings: ReplicatorSettings
  ) = Props(new DDataReplicator2(sharedMemoryMap, settings)).withDispatcher(settings.dispatcher)
}

// format: off
/** It demonstrates how Akka Distributed Data can maintain state without a database. It uses Akka Distributed Data to
  * store data across the Akka cluster using CRDTs. As long as at least one node continues to run, the data will be
  * available, no database is needed. Nodes can be killed, upgraded, whatever.
  *
  * How we checks replication? Every node in the cluster runs an async independent consistency checker process. The
  * checker scans through all the local data in a continuous loop, and runs a consistency check (gossip) on each range.
  *
  *
  * Implemented types of repairs:
  *
  * a) Repairs occur on read path (if any level higher then ReadLocal is used).
  * Blocking read repair ensures read monotonicity (see “Session Models”) for quorum reads: as soon as the client reads
  * a specific value, subsequent reads return the value at least as recent as the one it has seen, since replica states
  * were repaired. If we’re not using quorums for reads, we lose this monotonicity guarantee as data might have not been
  * propagated to the target node by the time of a subsequent read. At the same time, blocking read repair sacrifices
  * availability, since repairs should be acknowledged by the target replicas and the read cannot return until they
  * respond.
  *
  * b) Periodic async repair.
  *
  * How does Cassandra's repair works? It does a full key walk that triggers manually and it maintains merkel trees.
  * Using merkel trees hashes it detects inconsistencies across replicas. When it detects inconsistencies it repairs the
  * values.
  *
  * To detect exactly which records differ between replica responses, some databases (for example, Cassandra) use
  * specialized iterators with merge listeners (RowIteratorMergeListener)
  * (https://fossies.org/linux/apache-cassandra/src/java/org/apache/cassandra/service/reads/repair/RowIteratorMergeListener.java),
  * (full scan ????) which reconstruct differences between the merged result and individual inputs. Its output is then
  * used by the coordinator to notify replicas about the missing data.
  *
  * Hinted Handoff: repair during write path | Apache Cassandra 3.0
  * https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/operations/opsRepairNodesHintedHandoff.html?hl=hinted%2Chandoff
  *
  * TODO:
  *
  * 1. Delete operations
  *
  * 2. A separate actor for messages (e.g. Status, Gossip) that can grow in uncontrolled fashion.
  *
  * 3. SHA-1 -> SHA-512 (https://www.cockroachlabs.com/blog/trust-but-verify-cockroachdb-checks-replication/)
  *
  * 4. BroadcastHub for fan-out (N subscribers)
  *
  * 5. Sink/Source Refs for replication
  *
  * 6. Delete Deltas
  *
  * 7. Look at Hash DAGs and BloomFilter
  * (https://martin.kleppmann.com/2020/12/02/bloom-filter-hash-graph-sync.html)
  * Byzantine Eventual Consistency and the Fundamental Limits of Peer-to-Peer Databases https://arxiv.org/abs/2012.00472
  * Byzantine Eventual Consistency - Martin Kleppmann (https://youtu.be/RhVQ2y8rwe0)
  *
  * 8.Invertible Bloom Filters - @matheus23 - Unconf: https://youtu.be/YNbcXlllOBQ
  *
  * 8. MerkleTree or BloomFilter
  *
  * 9. Bitmap Version Vectors or Version Vectors (array-based implementation).
  *
  *
  */
// format: on
final class DDataReplicator2(
  val sharedMemoryMap: akka.cluster.ddata.durable.raf.SharedMemoryLongMap,
  settings: ReplicatorSettings
) extends Actor
    with ActorLogging
    with Stash
    with Timers
    with SharedMemoryMapPruningSupport {

  import Replicator.Internal._
  import Replicator._
  import settings._

  val cluster           = Cluster(context.system)
  val selfAddress       = cluster.selfAddress
  val selfFromSystemUid = Some(selfUniqueAddress.longUid)

  override lazy val selfUniqueAddress = cluster.selfUniqueAddress
  override lazy val pruningMarkerTTL  = settings.pruningMarkerTimeToLive

  require(!cluster.isTerminated, "Cluster node must not be terminated")
  require(
    roles.subsetOf(cluster.selfRoles),
    s"This cluster member [$selfAddress] doesn't have all the roles [${roles.mkString(", ")}]"
  )

  private val payloadSizeAggregator = {
    val sizeExceeding  = settings.logDataSizeExceeding.getOrElse(Int.MaxValue)
    val remoteProvider = RARP(context.system).provider
    val remoteSettings = remoteProvider.remoteSettings
    val maxFrameSize =
      if (remoteSettings.Artery.Enabled) remoteSettings.Artery.Advanced.MaximumFrameSize
      else context.system.settings.config.getBytes("akka.remote.classic.netty.tcp.maximum-frame-size").toInt
    new PayloadSizeAggregator(log, sizeExceeding, maxFrameSize)
  }

  val serializer = SerializationExtension(context.system).serializerFor(classOf[DataEnvelope])

  // Start periodic gossip to random nodes in cluster
  timers.startTimerWithFixedDelay(context.self.path.toString + "_gsp", GossipTick, gossipInterval)
  timers.startTimerWithFixedDelay("clock", ClockTick, gossipInterval)

  val maxPruningDisseminationNanos = maxPruningDissemination.toNanos
  if (pruningInterval >= Duration.Zero)
    timers.startTimerWithFixedDelay("pruning", RemovedNodePruningTick, pruningInterval, pruningInterval)

  // cluster members, doesn't contain selfAddress, doesn't contain joining and weaklyUp
  var nodes: immutable.SortedSet[UniqueAddress] = immutable.SortedSet.empty

  // cluster members sorted by age, oldest first,, doesn't contain selfAddress, doesn't contain joining and weaklyUp
  // only used when prefer-oldest is enabled
  var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(Member.ageOrdering)

  // cluster weaklyUp members, doesn't contain selfAddress
  var weaklyUpNodes: immutable.SortedSet[UniqueAddress] = immutable.SortedSet.empty

  // cluster joining members, doesn't contain selfAddress
  var joiningNodes: immutable.SortedSet[UniqueAddress] = immutable.SortedSet.empty

  // up and weaklyUp members, doesn't contain joining and not selfAddress
  private def allNodes: immutable.SortedSet[UniqueAddress] = nodes.union(weaklyUpNodes)

  private def isKnownNode(node: UniqueAddress): Boolean =
    nodes(node) || weaklyUpNodes(node) || joiningNodes(node) || selfUniqueAddress == node

  var removedNodes: Map[UniqueAddress, Long] = Map.empty

  // all members sorted with the leader first
  var leader: TreeSet[Member] = TreeSet.empty(Member.leaderStatusOrdering)

  def isLeader: Boolean =
    leader.nonEmpty && leader.head.address == selfAddress && leader.head.status == MemberStatus.Up

  def nodesForReadWrite(): Vector[UniqueAddress] =
    if (settings.preferOldest) membersByAge.iterator.map(_.uniqueAddress).toVector else nodes.toVector

  // for pruning timeouts are based on clock that is only increased when all members are reachable
  var previousClockTime     = System.nanoTime()
  var allReachableClockTime = 0L
  var unreachable           = Set.empty[UniqueAddress]

  val repairOffset          = new AtomicInteger(0)
  val repairRoundInProgress = new AtomicBoolean(false)

  // val threeInProgress = new AtomicBoolean(false)
  // val threeOffset     = new AtomicInteger(0)

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[ReachabilityEvent])

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    sharedMemoryMap.close()
  }

  override def receive: Receive = {
    case msg: DestinationSystemUid =>
      msg.toSystemUid match {
        case Some(uid) if uid != selfUniqueAddress.longUid =>
          // When restarting a node with same host:port it is possible that a Replicator on another node
          // is sending messages to the restarted node even if it hasn't joined the same cluster.
          // Therefore we check that the message was intended for this incarnation and otherwise
          // it is discarded.
          log.info(
            "Ignoring message [{}] from [{}] intended for system uid [{}], self uid is [{}]",
            Logging.simpleName(msg),
            sender(),
            uid,
            selfUniqueAddress.longUid
          )
        case _ =>
          msg match {
            case Status(otherDigests, _, _, _, fromSystemUid) =>
              receiveStatus(otherDigests, fromSystemUid)
            case Gossip(updatedData, sendBack, _, fromSystemUid) =>
              receiveGossip(updatedData, sendBack, fromSystemUid)
          }
      }

    case msg: SendingSystemUid =>
      msg.fromNode match {
        case Some(fromNode) if !isKnownNode(fromNode) =>
          // When restarting a node with same host:port it is possible that a Replicator on another node
          // is sending messages to the restarted node even if it hasn't joined the same cluster.
          // Therefore we check that the message was from a known cluster member
          log.info("Ignoring message [{}] from [{}] unknown node [{}]", Logging.simpleName(msg), sender(), fromNode)
        case _ =>
          msg match {
            case Read(key, _) =>
              val local = getData(key)
              println(s"Read($key)=$local")
              sender() ! ReadResult(local)
            case Write(key, envelope, _) =>
              receiveWrite(key, envelope, sender())
            case DeltaPropagation(from, reply, deltas) =>
              receiveDeltaPropagation(sender(), from, reply, deltas)
          }
      }

    case Get(key, consistency, req)   => receiveGet(key, consistency, req)
    case u @ Update(key, writeC, req) => receiveUpdate(key, u.modify, writeC, req)
    case ReadRepair(key, envelope)    => receiveReadRepair(key, envelope, sender())

    case GossipTick => receiveGossipTick()
    case ClockTick  => receiveClockTick()

    case MemberJoined(m)     => receiveMemberJoining(m)
    case MemberWeaklyUp(m)   => receiveMemberWeaklyUp(m)
    case MemberUp(m)         => receiveMemberUp(m)
    case MemberRemoved(m, _) => receiveMemberRemoved(m)
    case evt: MemberEvent    => receiveOtherMemberEvent(evt.member)

    case UnreachableMember(m) => receiveUnreachable(m)
    case ReachableMember(m)   => receiveReachable(m)

    case RemovedNodePruningTick => receiveRemovedNodePruningTick()

    case SharedMemoryMapPruningSupport.StartPruning(removedNodesFromDisk) =>
      log.warning("*** StartPruning adds from disk [{}]", removedNodesFromDisk.mkString(","))
      removedNodesFromDisk.foreach { rn =>
        removedNodes = removedNodes.updated(rn, allReachableClockTime)
      }

      // initiate pruning for removed nodes
      val removed: scala.collection.immutable.Set[UniqueAddress] =
        removedNodes.iterator
          .collect { case (r, t) if (allReachableClockTime - t) > maxPruningDisseminationNanos => r }
          .to(immutable.Set)
      initPruning(removed)

    case SharedMemoryMapPruningSupport.PruningStep(key, envelope, removed, action) =>
      action match {
        case SharedMemoryMapPruningSupport.PruningAction.Initialized =>
        // log.warning("*** Pruning [{} -> {}] for {} inited", removed, selfUniqueAddress, key)
        case SharedMemoryMapPruningSupport.PruningAction.Performed =>
        // log.warning("*** Pruning [{} -> {}] for [{}] performed", removed, selfUniqueAddress, key)
      }
      setData(key, envelope)

    case SharedMemoryMapPruningSupport.PruningRoundInitialized =>
      // log.warning("Step 1. [PruningInitialized]")
      performPruning(allNodes)

    case SharedMemoryMapPruningSupport.DeleteObsoletePruningPerformed(key, envelope, removedAddresses) =>
      // if (ThreadLocalRandom.current().nextDouble() < .1)
      // log.warning("Pruning:Step 3. [DeleteObsoletePruningPerformed {}] from {}", removedAddresses.mkString(","), key)

      setData(key, envelope)
      removedAddresses.foreach(ua => removedNodes -= ua)

    case SharedMemoryMapPruningSupport.PruningRoundFinished =>
      log.info("************** PruningRoundFinished **************")

    case Subscribe(key, ref) =>
    case Delete(key, consistency, req) =>
      ???
    case DeltaPropagationTick =>
      ???
  }

  def handleLocalValue(
    key: KeyR,
    localValue: Option[DataEnvelope],
    consistency: ReadConsistency,
    req: Option[Any],
    caller: ActorRef
  ): Unit = {
    log.warning("Received Get for key [{}].", key)
    if (isLocalGet(consistency)) {
      val reply = localValue match {
        case Some(DataEnvelope(DeletedData, _, _)) => GetDataDeleted(key, req)
        case Some(DataEnvelope(data, _, _))        => GetSuccess(key, req)(data)
        case None                                  => NotFound(key, req)
      }
      caller ! reply
    } else {
      context.actorOf(
        ReadAggregator
          .props(
            key,
            consistency,
            req,
            selfUniqueAddress,
            nodesForReadWrite(),
            unreachable,
            !settings.preferOldest,
            localValue,
            caller
          )
          .withDispatcher(context.props.dispatcher)
      )
    }
  }

  def receiveGet(key: KeyR, consistency: ReadConsistency, req: Option[Any]): Unit = {
    val replyTo         = sender()
    val maybeLocalValue = getData(key.id)
    // log.warning("Get [{}] = localValue:  {}", key, maybeLocalValue)

    if (isLocalGet(consistency)) {
      val reply = maybeLocalValue match {
        case Some(DataEnvelope(DeletedData, _, _)) => GetDataDeleted(key, req)
        case Some(DataEnvelope(data, _, _))        => GetSuccess(key, req)(data)
        case None                                  => NotFound(key, req)
      }
      replyTo ! reply
    } else {
      context.actorOf(
        ReadAggregator
          .props(
            key,
            consistency,
            req,
            selfUniqueAddress,
            nodesForReadWrite(),
            unreachable,
            !settings.preferOldest,
            maybeLocalValue,
            replyTo
          )
          .withDispatcher(context.props.dispatcher)
      )
    }
  }

  def isLocalGet(readConsistency: ReadConsistency): Boolean =
    readConsistency match {
      case ReadLocal                    => true
      case _: ReadMajority | _: ReadAll => nodes.isEmpty
      case _                            => false
    }

  def receiveUpdate(
    key: KeyR,
    modify: Option[ReplicatedData] => ReplicatedData,
    writeConsistency: WriteConsistency,
    req: Option[Any]
  ): Unit = {
    val replyTo = sender()
    Try {
      val localValue = getData(key.id)
      localValue match {
        case Some(envelope @ DataEnvelope(DeletedData, _, _)) =>
          envelope
        case Some(envelope @ DataEnvelope(existing, _, _)) =>
          modify(Some(existing)) match {
            case d: DeltaReplicatedData =>
              envelope.merge(d.resetDelta.asInstanceOf[existing.T])
            case d =>
              envelope.merge(d.asInstanceOf[existing.T])
          }
        case None =>
          val updated = modify(None)
          DataEnvelope(updated)
      }
    } match {
      case Success(DataEnvelope(DeletedData, _, _)) =>
        log.debug("Received Update for deleted key [{}].", key)
        replyTo ! UpdateDataDeleted(key, req)
      case Success(envelope) =>
        log.debug("Received Update for key [{}].", key)
        val updatedEnvelope = setData(key.id, envelope)
        replyTo ! UpdateSuccess(key, req)

      case Failure(e) =>
        log.debug("Received Update for key [{}], failed: {}", key, e.getMessage)
        replyTo ! ModifyFailure(key, "Update failed: " + e.getMessage, e, req)
    }
  }

  def isLocalUpdate(writeConsistency: WriteConsistency): Boolean =
    writeConsistency match {
      case WriteLocal                     => true
      case _: WriteMajority | _: WriteAll => nodes.isEmpty
      case _                              => false
    }

  def receiveWrite(key: KeyId, envelope: DataEnvelope, s: ActorRef): Unit =
    writeAndStore(key, envelope, s, reply = true)

  def writeAndStore(key: KeyId, writeEnvelope: DataEnvelope, replyTo: ActorRef, reply: Boolean): Option[DataEnvelope] =
    write(key, writeEnvelope) match {
      case v @ Some(_) =>
        if (reply) replyTo ! WriteAck
        v
      case None =>
        if (reply) replyTo ! WriteNack
        None
    }

  def write(key: KeyId, thatEnvelope: DataEnvelope): Option[DataEnvelope] =
    getData(key) match {
      case someEnvelope @ Some(envelope) if envelope eq thatEnvelope =>
        someEnvelope
      case Some(DataEnvelope(DeletedData, _, _)) => Some(DeletedEnvelope) // already deleted
      case Some(envelope @ DataEnvelope(existingData @ _, _, _)) =>
        try {
          val merged =
            (if (envelope.pruning.size > thatEnvelope.pruning.size) envelope.merge(thatEnvelope)
             else thatEnvelope.merge(envelope)).addSeen(selfAddress)
          Some(setData(key, merged))
        } catch {
          case e: IllegalArgumentException =>
            log.warning("Couldn't merge [{}], due to: {}", key, e.getMessage)
            None
        }
      case None =>
        // no existing data for the key
        val writeEnvelope2 = thatEnvelope.data match {
          case d: ReplicatedDelta =>
            val z = d.zero
            thatEnvelope.copy(data = z.mergeDelta(d.asInstanceOf[z.D]))
          case _ =>
            thatEnvelope
        }
        val writeEnvelope3 = writeEnvelope2.addSeen(selfAddress)
        Some(setData(key, writeEnvelope3))
    }

  def receiveReadRepair(key: KeyId, writeEnvelope: DataEnvelope, replyTo: ActorRef): Unit = {
    writeAndStore(key, writeEnvelope, replyTo, reply = false)
    replyTo ! ReadRepairAck
  }

  def setData(key: KeyId, envelope: DataEnvelope): DataEnvelope = {
    val digest0 =
      if (envelope.data == DeletedData) DDataReplicator2.DeletedDig
      else {
        val (d, size) = digest(envelope)
        payloadSizeAggregator.updatePayloadSize(key, size)
        d
      }

    // TODO: Handle one.nio.mem.OutOfMemoryException
    sharedMemoryMap.put(vehicleId2Long(key), new SharedMemoryValue(envelope, digest0))
    log.info("*** FLUSHED {}", envelope.data)
    envelope
  }

  def getLocalDigest(key: KeyId): Array[Byte] =
    Option(sharedMemoryMap.get(vehicleId2Long(key))) match {
      case Some(v) =>
        payloadSizeAggregator.updatePayloadSize(key, v.digest.size)
        // log.error(s"get: {$key -> ${bytes2Hex(v.digest)}}")
        v.digest
      case None => DDataReplicator2.NotFoundDig
    }

  /** @return
    *   SHA-512 digest of the serialized data, and the size of the serialized data
    */
  def digest(envelope: DataEnvelope): (Array[Byte], Int) =
    if (envelope.data == DeletedData) (DDataReplicator2.DeletedDig, 0)
    else {
      val bytes = serializer.toBinary(envelope.withoutDeltaVersions)
      val dig   = MessageDigest.getInstance("SHA-512").digest(bytes)
      (dig, bytes.length)
    }

  def getData(key: KeyId): Option[DataEnvelope] =
    Option(sharedMemoryMap.get(vehicleId2Long(key))).map(_.envelope.asInstanceOf[DataEnvelope])

  def getDeltaSeqNr(key: KeyId, fromNode: UniqueAddress): Long =
    Option(sharedMemoryMap.get(vehicleId2Long(key))) match {
      case Some(v) => v.envelope.asInstanceOf[DataEnvelope].deltaVersions.versionAt(fromNode)
      case None    => 0L
    }

  def isNodeRemoved(node: UniqueAddress, keys: Iterable[KeyId]): Boolean =
    removedNodes.contains(node) || keys.exists(key =>
      Option(sharedMemoryMap.get(vehicleId2Long(key))) match {
        case Some(v) => v.envelope.asInstanceOf[DataEnvelope].pruning.contains(node)
        case None    => false
      }
    )

  def receiveGossipTick(): Unit = {
    if (ThreadLocalRandom.current().nextDouble() > 0.6) {
      log.warning(
        s"""
           |Memory [Total:${sharedMemoryMap.getTotalMemory}, Used: ${sharedMemoryMap.getUsedMemory}] MaxCapacity:${sharedMemoryMap.getCapacity} Count:${sharedMemoryMap.getCount}
           |Storage [Free ${(sharedMemoryMap.getFreeMemory * 100) / sharedMemoryMap.getTotalMemory}% | Used ${(sharedMemoryMap.getUsedMemory * 100) / sharedMemoryMap.getTotalMemory}%]
           |""".stripMargin
      )
    }

    // buildTree()
    selectRandomNode(allNodes.toVector).foreach(gossipTo(_))
  }

  /*def buildTree(): Unit = {
    val size = sharedMemoryMap.getCount()
    if (threeInProgress.compareAndSet(false, true)) {
      val prevOffset = threeOffset.get()
      sharedMemoryMap.collectRangeDigest(prevOffset, maxDeltaElements).thenCompose { data =>
        CompletableFuture.supplyAsync { () =>
          if (data.getOffset > prevOffset) {
            threeOffset.set(data.getOffset)

            var arrayOfBlocks = Array.ofDim[Array[Byte]](size)
            data.getResults.forEach { (k, bts) =>
              arrayOfBlocks = arrayOfBlocks :+ bts
            }
            println(MerkleTree.fromArray(arrayOfBlocks)(MerkleDigest.SHA512).digest.toString)
          } else {
            log.info("Send Status: Start from the beginning")
            threeOffset.set(0)
            // replicationCheckerOffset.set(0)
          }
        }
      }
    }
  }*/

  def gossipTo(address: UniqueAddress): Unit = {
    val to          = replica(address)
    val toSystemUid = Some(address.longUid)
    val replicator  = self

    if (repairRoundInProgress.compareAndSet(false, true)) {
      val prevOffset = repairOffset.get()
      sharedMemoryMap.collectRangeDigest(prevOffset, maxDeltaElements).thenCompose { data =>
        CompletableFuture.supplyAsync(
          () => {
            if (data.getOffset > prevOffset) {
              repairOffset.set(data.getOffset)

              var digests = immutable.Map.empty[String, Digest]
              if (data.getResults.size() > 0) {
                data.getResults.forEach { (k, v) =>
                  digests = digests + (k -> ByteString.fromArrayUnsafe(v))
                }

                // akka.util.TypedMultiMap.empty[Int, String]
                val status = Status(digests, 0, 0, toSystemUid, selfFromSystemUid)
                to.tell(status, replicator)
              }
            } else {
              log.info("Send Status: Start from the beginning")
              repairOffset.set(0)
            }
            repairRoundInProgress.compareAndSet(true, false)
          },
          AsyncExecutor.POOL
        )
      }
    } else log.warning("Skip Status round. MMAP access takes longer than {}", gossipInterval)
  }

  def selectRandomNode(addresses: immutable.IndexedSeq[UniqueAddress]): Option[UniqueAddress] =
    if (addresses.isEmpty) None else Some(addresses(ThreadLocalRandom.current.nextInt(addresses.size)))

  def replica(node: UniqueAddress): ActorSelection =
    context.actorSelection(self.path.toStringWithAddress(node.address))

  def receiveStatus(thatDigests: Map[KeyId, Digest], fromSystemUid: Option[Long]): Unit = {
    val replyTo = sender()
    log.debug(
      "Received gossip status from [{}] containing [{}].",
      replyTo.path.address,
      thatDigests.keys.mkString(", ")
    )

    def digestStatus(key: KeyId, otherDigest: Array[Byte]): DDataReplicator2.DigestStatus = {
      val digest = getLocalDigest(key)

      val notFound = java.util.Arrays.equals(digest, DDataReplicator2.NotFoundDig)
      if (!notFound && !java.util.Arrays.equals(digest, otherDigest)) DDataReplicator2.DigestStatus.Different
      else if (notFound) DDataReplicator2.DigestStatus.NotFound
      else DDataReplicator2.DigestStatus.Same
    }

    val differentKeys = new ArrayBuffer[String]()
    val missingKeys   = new ArrayBuffer[String]

    thatDigests.foreach { case (key, digest) =>
      digestStatus(key, digest.toArrayUnsafe()) match {
        case DDataReplicator2.DigestStatus.Same      =>
        case DDataReplicator2.DigestStatus.Different => differentKeys += key
        case DDataReplicator2.DigestStatus.NotFound  => missingKeys += key
      }
    }

    // log.info("different:{} missing:{}", differentKeys.mkString(","), missingKeys.mkString(","))

    if (differentKeys.nonEmpty) {
      val keysToGossip = differentKeys
      log.warning("Sending gossip to [{}], containing [{}]", replyTo.path.address, keysToGossip.mkString(","))
      createGossipMessages(keysToGossip.iterator, true, fromSystemUid).foreach { g =>
        replyTo ! g
      }
    }

    if (missingKeys.nonEmpty) {
      log.warning(
        "Requesting missing [{}]",
        // replyTo.path.address,
        missingKeys.mkString(", ")
      )

      val status = Status(missingKeys.map(k => k -> NotFoundDigest).toMap, 0, 0, fromSystemUid, selfFromSystemUid)
      replyTo ! status
    }
  }

  def createGossipMessages(
    keys: Iterator[KeyId],
    sendBack: Boolean,
    fromSystemUid: Option[Long]
  ): Vector[Gossip] = {
    // The sizes doesn't have to be exact, rather error on too small messages. The serializer is also
    // compressing the Gossip message.
    val maxMessageSize = payloadSizeAggregator.maxFrameSize - 128

    var messages         = Vector.empty[Gossip]
    val collectedEntries = Vector.newBuilder[(KeyId, DataEnvelope)]
    var sum              = 0

    def addGossip(): Unit = {
      val entries = collectedEntries.result().toMap
      if (entries.nonEmpty) {
        messages :+= Gossip(entries, sendBack, fromSystemUid, selfFromSystemUid)
      }
    }

    keys.foreach { key =>
      val keySize = key.length + 4
      val dataSize = payloadSizeAggregator.getMaxSize(key) match {
        case 0 =>
          payloadSizeAggregator.getMaxSize(key)
        case size => size
      }
      val dataEnvelope = getData(key).get
      val envelopeSize = 100 + dataEnvelope.estimatedSizeWithoutData

      val entrySize = keySize + dataSize + envelopeSize
      if (sum + entrySize <= maxMessageSize) {
        collectedEntries += (key -> dataEnvelope)
        sum += entrySize
      } else {
        addGossip()
        collectedEntries.clear()
        collectedEntries += (key -> dataEnvelope)
        sum = entrySize
      }
    }

    addGossip()

    // Created [1] Gossip messages from [102] data entries.

    // Created [1] Gossip messages from [0] data entries. ??????
    log.warning("Created [{}] Gossip messages from [{}] data entries.", messages.size, keys.size)

    messages
  }

  def receiveGossip(updatedData: Map[KeyId, DataEnvelope], sendBack: Boolean, fromSystemUid: Option[Long]): Unit = {
    val replyTo = sender()
    log.warning(
      "Received gossip from [{}], containing [{}].",
      replyTo.path.address,
      updatedData.keys.mkString(", ")
    )
    var replyKeys = Set.empty[KeyId]
    updatedData.foreach { case (key, thatEnvelope) =>
      val prev = sharedMemoryMap.get(vehicleId2Long(key))

      /*val thatEnvelope0 =
        if (prev != null) {
          val prevEnv = prev.envelope.asInstanceOf[DataEnvelope].data.asInstanceOf[VoteEnvelope]
          val that    = thatEnvelope.data.asInstanceOf[VoteEnvelope]
          println(s"Current: $prevEnv - Incoming: $that")

          /*val r =
            if (prevEnv.epoch < that.epoch) that.addSeen(selfUniqueAddress)
            else if (prevEnv.epoch > that.epoch) prevEnv
            else that.merge(prevEnv)
          DataEnvelope(r)
       */
          thatEnvelope.merge(DataEnvelope(prevEnv))
        } else {
          thatEnvelope.merge(DataEnvelope(VoteEnvelope().addSeen(selfUniqueAddress)))
        }*/

      /*val thatEnvelope0 =
        if (prev != null) {
          val prevEnv = prev.envelope.asInstanceOf[DataEnvelope].data.asInstanceOf[HighWatermark]
          val that    = thatEnvelope.data.asInstanceOf[HighWatermark]

          val pr = prevEnv.state.map { case (a, c) => s"${a.address.host.getOrElse("")}@$c" }.mkString(", ")
          val th = that.state.map { case (a, c) => s"${a.address.host.getOrElse("")}@$c" }.mkString(", ")

          val merged = that.localMerge(selfUniqueAddress, that, cluster.state.members.map(_.uniqueAddress))
          // val merged = prevEnv.merge(that).localMerge(selfUniqueAddress, that, cluster.state.members.map(_.uniqueAddress))

          val md = merged.state.map { case (a, c) => s"${a.address.host.getOrElse("")}@$c" }.mkString(", ")

          println(s"Current: ($pr) vs Incoming: ($th) == ($md)")

          thatEnvelope.copy(merged)
        } else {
          val that = thatEnvelope.data.asInstanceOf[HighWatermark]
          val str  = that.state.map { case (a, c) => s"${a.address.host.getOrElse("")}@$c" }.mkString(", ")
          println(s"Current: (null) vs Incoming: ($str)")
          thatEnvelope
        }*/

      /*val prevStr =
        if (prev != null) {
          val prevEnv = prev.envelope.asInstanceOf[DataEnvelope].data.asInstanceOf[ReplicatedVehicle]
          prevEnv.replicationState.map { case (a, c) => s"${a.address.host.getOrElse("")}:$c" }.mkString(", ")
        } else "null"*/

      val that = thatEnvelope.data.asInstanceOf[ReplicatedVehicleRange]
      val merged = that.markSeen(
        selfUniqueAddress,
        Try(prev.envelope.asInstanceOf[DataEnvelope].data.asInstanceOf[ReplicatedVehicleRange]).toOption
      )

      val mergedEnvelope = thatEnvelope.copy(data = merged).addSeen(selfUniqueAddress.address)

      val contains = prev != null

      // read -> write
      val d = writeAndStore(key, mergedEnvelope, replyTo, reply = false)
      if (sendBack) {
        d match {
          case Some(d) =>
            if (contains || d.pruning.nonEmpty)
              replyKeys += key
          case None =>
        }
      }
    }
    if (sendBack && replyKeys.nonEmpty) {
      createGossipMessages(replyKeys.iterator, sendBack = false, fromSystemUid).foreach { g =>
        replyTo ! g
      }
    }
  }

  def receiveMemberJoining(m: Member): Unit =
    if (m.address != selfAddress)
      joiningNodes += m.uniqueAddress

  def receiveMemberWeaklyUp(m: Member): Unit =
    if (m.address != selfAddress) {
      weaklyUpNodes += m.uniqueAddress
      joiningNodes -= m.uniqueAddress
    }

  def receiveMemberUp(m: Member): Unit = {
    leader += m
    if (m.address != selfAddress) {
      nodes += m.uniqueAddress
      weaklyUpNodes -= m.uniqueAddress
      joiningNodes -= m.uniqueAddress
      if (settings.preferOldest)
        membersByAge += m
    }
  }

  def receiveMemberRemoved(m: Member): Unit =
    if (m.address == selfAddress)
      context.stop(self)
    else {
      log.debug("adding removed node [{}] from MemberRemoved", m.uniqueAddress)
      // filter, it's possible that the ordering is changed since it based on MemberStatus
      leader = leader.filterNot(_.uniqueAddress == m.uniqueAddress)
      nodes -= m.uniqueAddress
      weaklyUpNodes -= m.uniqueAddress
      joiningNodes -= m.uniqueAddress
      removedNodes = removedNodes.updated(m.uniqueAddress, allReachableClockTime)
      unreachable -= m.uniqueAddress
      if (settings.preferOldest)
        membersByAge -= m
    }

  def receiveOtherMemberEvent(m: Member): Unit = {
    // replace, it's possible that the ordering is changed since it based on MemberStatus
    leader = leader.filterNot(_.uniqueAddress == m.uniqueAddress)
    leader += m
  }

  def receiveUnreachable(m: Member): Unit =
    unreachable += m.uniqueAddress

  def receiveReachable(m: Member): Unit =
    unreachable -= m.uniqueAddress

  def receiveClockTick(): Unit = {
    val now = System.nanoTime()
    if (unreachable.isEmpty)
      allReachableClockTime += (now - previousClockTime)
    previousClockTime = now
  }

  def receiveDeltaPropagation(
    replyTo: ActorRef,
    fromNode: UniqueAddress,
    reply: Boolean,
    deltas: Map[KeyId, Delta]
  ): Unit =
    try {
      val isDebugEnabled = log.isDebugEnabled
      if (isDebugEnabled)
        log.debug(
          "Received DeltaPropagation from [{}], containing [{}].",
          fromNode.address,
          deltas.collect { case (key, Delta(_, fromSeqNr, toSeqNr)) => s"$key $fromSeqNr-$toSeqNr" }.mkString(", ")
        )

      if (isNodeRemoved(fromNode, deltas.keys)) {
        // Late message from a removed node.
        // Drop it to avoid merging deltas that have been pruned on one side.
        if (isDebugEnabled)
          log.debug("Skipping DeltaPropagation from [{}] because that node has been removed", fromNode.address)
      } else {
        deltas.foreach {
          case (key, Delta(envelope @ DataEnvelope(_: RequiresCausalDeliveryOfDeltas, _, _), fromSeqNr, toSeqNr)) =>
            val currentSeqNr = getDeltaSeqNr(key, fromNode)
            if (currentSeqNr >= toSeqNr) {
              if (isDebugEnabled)
                log.debug(
                  "Skipping DeltaPropagation from [{}] for [{}] because toSeqNr [{}] already handled [{}]",
                  fromNode.address,
                  key,
                  toSeqNr,
                  currentSeqNr
                )
              if (reply) replyTo ! WriteAck
            } else if (fromSeqNr > (currentSeqNr + 1)) {
              if (isDebugEnabled)
                log.debug(
                  "Skipping DeltaPropagation from [{}] for [{}] because missing deltas between [{}-{}]",
                  fromNode.address,
                  key,
                  currentSeqNr + 1,
                  fromSeqNr - 1
                )
              if (reply) replyTo ! DeltaNack
            } else {
              if (isDebugEnabled)
                log.debug(
                  "Applying DeltaPropagation from [{}] for [{}] with sequence numbers [{}], current was [{}]",
                  fromNode.address,
                  key,
                  s"$fromSeqNr-$toSeqNr",
                  currentSeqNr
                )
              val newEnvelope = envelope.copy(deltaVersions = VersionVector(fromNode, toSeqNr))
              writeAndStore(key, newEnvelope, replyTo, reply)
            }
          case (key, Delta(envelope, _, _)) =>
            // causal delivery of deltas not needed, just apply it
            writeAndStore(key, envelope, replyTo, reply)
        }
      }
    } catch {
      case NonFatal(e) =>
        // catching in case we need to support rolling upgrades that are
        // mixing nodes with incompatible delta-CRDT types
        log.warning("Couldn't process DeltaPropagation from [{}] due to {}", fromNode, e)
    }

  def receiveRemovedNodePruningTick(): Unit =
    if (unreachable.isEmpty) {
      // log.info("************** PruningRoundStarted **************")
      if (isLeader) {
        // Up and Removed members addresses the leader knows about.
        val knownNodes = allNodes.union(removedNodes.keySet)
        log.info("************** PruningRoundStarted: {}", knownNodes.mkString(","))
        collectRemovedNodes(knownNodes)
      } else {
        deleteObsoletePruningMarkers()
      }
    }
}
