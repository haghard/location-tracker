package akka.cluster.ddata.replicator

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.Props
import akka.actor.Stash
import akka.actor.Timers
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.ClusterEvent.*
import akka.cluster.*
import akka.cluster.ddata.BloomFilterGuardian.BFCmd
import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Key.KeyR
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.cluster.ddata.*
import akka.cluster.ddata.replicator.DDataReplicator.DigestStatus
import akka.cluster.ddata.replicator.DDataReplicatorRocksDB.DataEnvelopeManifest
import akka.cluster.ddata.replicator.DDataReplicatorRocksDB.DataEnvelopeOps
import akka.cluster.ddata.replicator.SharedMemoryMapPruningSupport.PerformPruning
import akka.cluster.ddata.utils.MerkleDigest
import akka.cluster.ddata.utils.MerkleTree
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.remote.RARP
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.stream.Attributes
import akka.stream.BoundedSourceQueue
import akka.stream.ClosedShape
import akka.stream.Graph
import akka.stream.QueueCompletionResult
import akka.stream.QueueOfferResult
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.google.common.primitives.Longs
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.ReadOptions
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions

import java.security.MessageDigest
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable
import scala.collection.immutable.TreeSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import DDataReplicatorRocksDB.*

object DDataReplicatorRocksDB {

  val BF_KEY       = "bf"
  val SIG          = "SHA3-256"
  val dbDispatcher = "rocks-db-dispatcher"

  implicit final class DataEnvelopeOps(val self: DataEnvelope) extends AnyVal {
    def addSelf(selfUniqueAddress: UniqueAddress): DataEnvelope = {
      val vehicle = self.data.asInstanceOf[ReplicatedVehicle]
      val updated =
        vehicle.copy(replicationState = vehicle.replicationState.updated(selfUniqueAddress, vehicle.version))
      self.copy(data = updated)
    }
  }

  def readLocal(envelopeBts: Array[Byte], serializer: SerializerWithStringManifest): ReplicatedVehicle =
    serializer
      .fromBinary(envelopeBts, DataEnvelopeManifest)
      .asInstanceOf[DataEnvelope]
      .data
      .asInstanceOf[ReplicatedVehicle]

  def vehicleId2Long(vehicleId: String): Long =
    // OR com.google.common.primitives.Longs.tryParse(vehicleId)
    java.lang.Long.parseLong(vehicleId)

  val NotFoundDig = Array[Byte](-1)
  val DeletedDig  = Array.emptyByteArray

  sealed trait DigestStatus

  object DigestStatus {
    case object Different extends DigestStatus

    case object Same extends DigestStatus

    case object NotFound extends DigestStatus
  }

  // ReplicatorMessageSerializer.DataEnvelopeManifest
  val DataEnvelopeManifest = "H"

  sealed trait RMsg
  object RMsg {
    final case class External(msg: RExternal) extends RMsg
    final case class Internal(msg: RInternal) extends RMsg
  }

  sealed trait RExternal
  object RExternal {
    final case class Upd(r: ActorRef, u: akka.cluster.ddata.Replicator.Update[akka.cluster.ddata.ReplicatedData])
        extends RExternal
  }

  sealed trait RInternal
  object RInternal {
    final case class Status(r: ActorRef, msg: akka.cluster.ddata.Replicator.Internal.Status) extends RInternal
    final case class Gossip(r: ActorRef, msg: akka.cluster.ddata.Replicator.Internal.Gossip) extends RInternal
    final case class KeysSetDiff(newKeys: mutable.Set[Long], replyTo: ActorRef, fromSystemUid: Option[Long])
        extends RInternal
  }

  private def showSummary(
    message: String,
    log: LoggingAdapter,
    duration: FiniteDuration
  ): Sink[RMsg, akka.NotUsed] =
    Flow[RMsg]
      .conflateWithSeed(_ => (0L, 0L)) { case ((external, internal), m) =>
        m match {
          case RMsg.External(_) => (external + 1L, internal)
          case RMsg.Internal(_) => (external, internal + 1L)
        }
      }
      .zipWith(Source.tick(duration, duration, ()))(Keep.left)
      .scan((0L, 0L)) { case ((external, internal), (a, b)) => (external + a, internal + b) }
      .to(Sink.foreach { case (external, internal) => log.info(s"$message: [Ext:$external/Int:$internal]") })
      .withAttributes(Attributes.inputBuffer(1, 1))

  // If both have elements available, prefer the 'externalSrc' when 'preferred' is 'true'
  def graph(
    source: Source[RMsg, akka.NotUsed],
    replicator: ActorRef,
    onUpdate: (ActorRef, KeyR, Option[ReplicatedData] => ReplicatedData, Option[Any]) => Unit,
    writeParallelism: Int
  )(implicit ec: ExecutionContext): Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val twoWayRouter = b.add(
        TwoWayRouter[RMsg, RExternal, RInternal] { msg: RMsg =>
          msg match {
            case RMsg.Internal(msg) => Right(msg)
            case RMsg.External(msg) => Left(msg)
          }
        }.addAttributes(Attributes.inputBuffer(0, 0))
      )

      // Gossip|Status|KeysSetDiff. No backpressure, externalSink2 should backpressure this one
      val internalSink = Sink.foreach[RInternal](replicator ! _)

      // Low latency sink, writes in parallel.
      // If it's a problem for you (to avoid LWW) you can:
      // 1. writeParallelism=1
      // 2. use RocksDb transactions to prevent concurrent updates.
      val externalSink2: Sink[RExternal, Future[akka.Done]] =
        Sink.foreachAsync[RExternal](writeParallelism) { ext: RExternal =>
          Future {
            ext match {
              case RExternal.Upd(replyTo, update) =>
                onUpdate(replyTo, update.key, update.modify, update.request)
            }
          }
        }

      // high throughput sink
      /*val updateSink: Sink[Seq[RExternal], Future[akka.Done]] =
        Sink.foreachAsync[Seq[RExternal]](1) { batch: Seq[RExternal] =>
          Future {
            batch.foreach { ext =>
              ext match {
                case RExternal.Upd(replyTo, update) =>
                  onUpdate(replyTo, update.key, update.modify, update.request)
              }
            }
          }
        }*/

      /*
      val buffer = b.add(
        Flow[RMsg]
          .collect { case RMsg.External(upd) => upd }
          .buffer(4, OverflowStrategy.backpressure)
          // .groupedWithin(4, 50.millis)
      )
      val filter = b.add(Flow[RMsg].collect { case RMsg.Internal(msg) => msg })
      val bcast  = b.add(akka.stream.scaladsl.Broadcast[RMsg](2))

      // format: off
      source ~> bcast ~> filter ~> internalSink
                bcast ~> buffer ~> updateSink2 //updateSink
      // format: on
       */

      // format: off
      source ~> twoWayRouter.in
                twoWayRouter.out0 ~> externalSink2
                twoWayRouter.out1 ~> internalSink
      // format: on
      // externalSink2 backpressures internalSink
      ClosedShape
    }

  def props(
    bf: akka.actor.typed.ActorRef[BFCmd],
    internalQueue: BoundedSourceQueue[RMsg.Internal],
    internalSrc: Source[RMsg.Internal, akka.NotUsed],
    db: RocksDB,
    columnFamily: ColumnFamilyHandle,
    settings: ReplicatorSettings
  ) = Props(
    new DDataReplicatorRocksDB(
      internalQueue,
      internalSrc,
      db,
      columnFamily,
      settings,
      bf
    )
  ).withDispatcher(settings.dispatcher)
}

// format: off
/**
  * How we checks replication? Every node in the cluster runs an async independent consistency checker process. The
  * checker scans through all the local data in a continuous loop, and runs a consistency check (gossip) on each range.
  * On top ot that, as an optimization we have [[BloomFilterGuardian]] that sends a BloomFilter that contains all keys to speed up replication.
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
  *
  * Other examples:
  * https://github.com/thatdot/quine/blob/main/quine-rocksdb-persistor/src/main/scala/com/thatdot/quine/persistor/RocksDbPersistor.scala
  * https://github.com/alephium/alephium/blob/master/benchmark/src/main/scala/org/alephium/benchmark/RocksDBBench.scala
  *
  */
// format: on
final class DDataReplicatorRocksDB(
  internalQueue: BoundedSourceQueue[RMsg.Internal],
  internalSrc: Source[RMsg.Internal, akka.NotUsed],
  val db: RocksDB,
  val columnFamily: ColumnFamilyHandle,
  val settings: ReplicatorSettings,
  bf: akka.actor.typed.ActorRef[BFCmd],
  val writeOptions: WriteOptions = new WriteOptions().setDisableWAL(false).setSync(false)
  // writeAheadLog whether to enable the WAL (enable if you want to avoid data loss on crash)
  // syncWrites whether to sync fully to the OS the write (much slower, but no data loss on power failure)
) extends Actor
    with ActorLogging
    with Stash
    with Timers
    with PruningRockDb {

  import Replicator.Internal._
  import Replicator._
  import settings._

  val cluster           = Cluster(context.system)
  val selfAddress       = cluster.selfAddress
  val selfFromSystemUid = Some(selfUniqueAddress.longUid)

  implicit val sys = context.system.toTyped

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

  // akka.cluster.ddata.CustomReplicatorMessageSerializerUdp
  val serializer = SerializationExtension(context.system)
    .serializerFor(classOf[DataEnvelope])
    .asInstanceOf[SerializerWithStringManifest]

  val gossipMaxDeltaElements: Int = maxDeltaElements

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

  val (externalQ, externalSrc) = Source.queue[RMsg.External](128).preMaterialize()

  RunnableGraph
    .fromGraph(
      graph(
        internalSrc
          .mergePreferred(externalSrc, true)
          .alsoTo(showSummary("summary", log, 30.seconds)),
        self,
        receiveUpdate,
        RocksDbSettings.parallelism
      )(context.system.dispatchers.lookup(dbDispatcher))
    )
    .run()

  val lastGossiped   = new AtomicReference[Long](Long.MinValue)
  val lastMerkleTree = new AtomicReference[Long](Long.MinValue)

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[ReachabilityEvent])

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    db.cancelAllBackgroundWork(true)
    columnFamily.close()
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
            case s: Status =>
              val r = sender()
              internalQueue.offer(RMsg.Internal(RInternal.Status(r, s))) match {
                case _: QueueCompletionResult  =>
                case QueueOfferResult.Enqueued =>
                case QueueOfferResult.Dropped =>
                  log.warning("Dropped status [{}]", s.digests.keySet.take(5))
              }
            case g: Gossip =>
              val r = sender()
              internalQueue.offer(RMsg.Internal(RInternal.Gossip(r, g))) match {
                case _: QueueCompletionResult  =>
                case QueueOfferResult.Enqueued =>
                case QueueOfferResult.Dropped =>
                  log.warning("Dropped gossip [{}]", g.updatedData.keySet.take(5))
              }
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
              val maybeLocal = getData(vehicleId2Long(key))
              println(s"Read($key)=${maybeLocal.getOrElse("None")}")
              sender() ! ReadResult(maybeLocal)
            case Write(key, envelope, _) =>
              receiveWrite(vehicleId2Long(key), envelope, sender())
            case _: DeltaPropagation =>
          }
      }

    // Read
    case Get(key, consistency, req) =>
      receiveGet(key, consistency, req)

    case u @ Update(key, _, _) =>
      val replyTo = sender()
      externalQ.offer(RMsg.External(RExternal.Upd(replyTo, u))) match {
        case _: QueueCompletionResult  =>
        case QueueOfferResult.Enqueued =>
        case QueueOfferResult.Dropped =>
          log.warning("Dropped update [{}]", key)
      }

    case ReadRepair(key, envelope) =>
      val replyTo = sender()
      receiveReadRepair(vehicleId2Long(key), envelope, replyTo)

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

    case SharedMemoryMapPruningSupport.StartPruning(nodesToRemove) =>
      nodesToRemove.foreach { n =>
        removedNodes = removedNodes.updated(n, allReachableClockTime)
      }

      // initiate pruning for removed nodes
      val removed: scala.collection.immutable.Set[UniqueAddress] =
        removedNodes.iterator
          .collect { case (r, t) if (allReachableClockTime - t) > maxPruningDisseminationNanos => r }
          .to(immutable.Set)

      if (removed.nonEmpty) {
        log.warning(
          "Pruning: Step.1 Init {} Members: [{}, ...]",
          nodesToRemove.size,
          nodesToRemove.take(5).mkString(",")
        )

        implicit val ec = context.system.dispatchers.lookup(dbDispatcher)
        val keysToPrune = initPruning(removed)
        // wait for Gossip to propagate this change
        context.system.scheduler.scheduleOnce(gossipInterval + (gossipInterval / 2), self, PerformPruning(keysToPrune))
      }

    case PerformPruning(keysToPrune) =>
      val removedAddresses = deleteObsoletePruningMarkers(keysToPrune)

      val keysToCleanupMarker = performPruning(allNodes, keysToPrune)
      if (ThreadLocalRandom.current().nextDouble() > .7) {
        deleteObsoletePruningMarkers(keysToCleanupMarker)
        log.info(s"*** PruningRoundFinished(${removedAddresses.mkString(",")}) ***")
      }

    case internal: RInternal =>
      internal match {
        case RInternal.Status(replyTo, status) =>
          receiveStatus(replyTo, status.digests, status.fromSystemUid)
        case RInternal.Gossip(replyTo, gossip) =>
          receiveGossip(replyTo, gossip.updatedData, gossip.sendBack, gossip.fromSystemUid)
        case RInternal.KeysSetDiff(newKeys, replyTo, fromSystemUid) =>
          receiveBFStatus(replyTo, fromSystemUid, newKeys)
      }
    case _: Subscribe[_]      =>
    case _: Delete[_]         =>
    case DeltaPropagationTick =>
  }

  def receiveGet(key: KeyR, consistency: ReadConsistency, req: Option[Any]): Unit = {
    val replyTo         = sender()
    val maybeLocalValue = getData(vehicleId2Long(key.id))
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

  // read-modify-write
  def receiveUpdate(
    replyTo: ActorRef,
    key: KeyR,
    modify: Option[ReplicatedData] => ReplicatedData,
    req: Option[Any]
  ): Unit = {
    val k = vehicleId2Long(key.id)
    Try {
      val localValue = getData(k)
      localValue match {
        case Some(localEnvelope @ DataEnvelope(DeletedData, _, _)) =>
          localEnvelope
        case Some(localEnv @ DataEnvelope(existing, _, _)) =>
          modify(Some(existing)) match {
            case d: DeltaReplicatedData =>
              localEnv.merge(d.resetDelta.asInstanceOf[existing.T])
            case updated =>
              // 1. Lookup the existing value (A)
              // 2. val B = A.update() // inc version
              // 3. val C = A merge B
              // 4. save(C)
              localEnv.merge(updated.asInstanceOf[existing.T])
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
        // Updated value
        val updatedEnvelope = setData(k, envelope)
        replyTo ! UpdateSuccess(key, req)

      case Failure(e) =>
        log.debug("Received Update for key [{}], failed: {}", key, e.getMessage)
        replyTo ! ModifyFailure(key, "Update failed: " + e.getMessage, e, req)
    }
  }

  def receiveWrite(key: Long, envelope: DataEnvelope, s: ActorRef): Unit =
    writeAndStore(key, envelope, s, reply = true)

  def writeAndStore(key: Long, writeEnvelope: DataEnvelope, replyTo: ActorRef, reply: Boolean): Option[DataEnvelope] =
    write(key, writeEnvelope) match {
      case v @ Some(_) =>
        if (reply) replyTo ! WriteAck
        v
      case None =>
        if (reply) replyTo ! WriteNack
        None
    }

  def write(key: Long, thatEnvelope: DataEnvelope): Option[DataEnvelope] =
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

  def receiveReadRepair(key: Long, writeEnvelope: DataEnvelope, replyTo: ActorRef): Unit = {
    writeAndStore(key, writeEnvelope, replyTo, reply = false)
    replyTo ! ReadRepairAck
  }

  def setData(key: Long, envelope: DataEnvelope): DataEnvelope = {
    db.put(columnFamily, writeOptions, Longs.toByteArray(key), serializer.toBinary(envelope))
    bf.tell(BFCmd.PutKey(key))
    envelope
  }

  def getByKey(key: Long): Option[(DataEnvelope, Array[Byte])] = {
    val envelopeBts = db.get(columnFamily, Longs.toByteArray(key))
    if (envelopeBts ne null) {
      val env = serializer.fromBinary(envelopeBts, DataEnvelopeManifest).asInstanceOf[DataEnvelope]
      Some((env, MessageDigest.getInstance(SIG).digest(envelopeBts)))
    } else None
  }

  def getData(key: Long): Option[DataEnvelope] = {
    val envelopeBts = db.get(columnFamily, Longs.toByteArray(key))
    if (envelopeBts ne null) {
      Some(serializer.fromBinary(envelopeBts, DataEnvelopeManifest).asInstanceOf[DataEnvelope])
    } else None
  }

  def getDeltaSeqNr(key: Long, fromNode: UniqueAddress): Long =
    getData(key).map(_.deltaVersions.versionAt(fromNode)).getOrElse(0L)

  def isNodeRemoved(node: UniqueAddress, keys: Iterable[Long]): Boolean =
    removedNodes.contains(node) || keys.exists(key => getData(key).map(_.pruning.contains(node)).getOrElse(false))

  def receiveGossipTick(): Unit =
    /*if (ThreadLocalRandom.current().nextDouble() > 0.6) {
      log.warning(
        s"""
           |Memory [Total:${sharedMemoryMap.getTotalMemory}, Used: ${sharedMemoryMap.getUsedMemory}] MaxCapacity:${sharedMemoryMap.getCapacity} Count:${sharedMemoryMap.getCount}
           |Storage [Free ${(sharedMemoryMap.getFreeMemory * 100) / sharedMemoryMap.getTotalMemory}% | Used ${(sharedMemoryMap.getUsedMemory * 100) / sharedMemoryMap.getTotalMemory}%]
           |""".stripMargin
      )
    }*/
    selectRandomNode(allNodes.toVector).foreach { addr =>
      gossipTo(self, addr)
    // buildTree()
    // buildMerkleTree()
    }

  def buildMerkleTree(): Future[Unit] =
    Future {
      val snapshot = db.getSnapshot
      val readOps  = new ReadOptions().setSnapshot(snapshot)
      val iter     = db.newIterator(columnFamily, readOps)
      try {
        val startKey = lastMerkleTree.get()
        if (startKey == null) iter.seekToFirst()
        else iter.seek(Longs.toByteArray(startKey))

        var i      = maxDeltaElements
        val buffer = mutable.ArrayBuilder.make[Array[Byte]]

        while (i > 0)
          if (iter.isValid) {
            val key = Longs.fromByteArray(iter.key())
            buffer.+=(MessageDigest.getInstance(SIG).digest(iter.value()))
            lastMerkleTree.set(key)
            i = i - 1
            iter.next()
          } else {
            i = 0
            lastMerkleTree.set(Long.MinValue)
          }

        val arrays = buffer.result()
        val hex    = MerkleTree.fromArray(arrays)(MerkleDigest.Keccak256).digest.toString

        val hex2 = akka.cluster.ddata.replicator.mtree.JMerkleTree
          .treeFromScala(
            arrays.asInstanceOf[Array[AnyRef]],
            (bts: Array[Byte]) => new org.bouncycastle.jcajce.provider.digest.Keccak.Digest256().digest(bts)
          )
          .getHashStr

        // println(s" [$startKey...${lastMerkleTree.get()}] = Merkle($hex, $hex2)")

      } finally
        try iter.close()
        finally
          try readOps.close()
          finally
            db.releaseSnapshot(snapshot)

    }(context.system.dispatchers.lookup(dbDispatcher))

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

  def gossipTo(
    replicator: ActorRef,
    address: UniqueAddress
  ) =
    if (ThreadLocalRandom.current().nextDouble() < .5) {
      val dest = replica(address)
      bf.tell(BFCmd.SendStatus(dest, replicator, address.longUid, selfFromSystemUid))
    } else {
      Future {
        val dest     = replica(address)
        val snapshot = db.getSnapshot
        val readOps  = new ReadOptions().setSnapshot(snapshot)
        val iter     = db.newIterator(columnFamily, readOps)
        try {
          val lastGossipedKey = lastGossiped.get()
          if (lastGossipedKey == Long.MinValue) iter.seekToFirst()
          else iter.seek(Longs.toByteArray(lastGossipedKey))

          var i       = maxDeltaElements
          var digests = immutable.Map.empty[String, Digest]
          while (i > 0)
            if (iter.isValid) {
              val key    = Longs.fromByteArray(iter.key())
              val digBts = MessageDigest.getInstance(SIG).digest(iter.value())
              digests = digests + (key.toString -> ByteString.fromArrayUnsafe(digBts))

              lastGossiped.set(key)
              i = i - 1
              iter.next()
            } else {
              i = 0
              lastGossiped.set(Long.MinValue)
            }

          val status = Status(digests, 0, 0, Some(address.longUid), selfFromSystemUid)
          dest.tell(status, replicator)
        } finally
          try iter.close()
          finally
            try readOps.close()
            finally
              db.releaseSnapshot(snapshot)

      }(context.system.dispatchers.lookup(dbDispatcher))
    }

  def selectRandomNode(addresses: immutable.IndexedSeq[UniqueAddress]): Option[UniqueAddress] =
    if (addresses.isEmpty) None else Some(addresses(ThreadLocalRandom.current.nextInt(addresses.size)))

  def replica(node: UniqueAddress): ActorSelection =
    context.actorSelection(self.path.toStringWithAddress(node.address))

  def receiveStatus(
    replyTo: ActorRef,
    thatDigests: Map[KeyId, Digest],
    fromSystemUid: Option[Long]
  ): Unit =
    thatDigests.get(BF_KEY) match {
      case Some(otherBF) =>
        bf.tell(BFCmd.EvalDiff(otherBF, replyTo, fromSystemUid))
      case None =>
        receiveStatus0(replyTo, thatDigests, fromSystemUid)
    }

  def receiveBFStatus(
    replyTo: ActorRef,
    fromSystemUid: Option[Long],
    additional: mutable.Set[Long]
  ): Unit = {

    val different: mutable.Map[String, DataEnvelope] = new mutable.HashMap()
    additional.foreach { key =>
      getByKey(key).foreach { case (env, _) =>
        different.put(key.toString, env)
      }
    }

    if (different.nonEmpty) {
      log.warning(
        "Sending BF gossip to {}, containing {} keys [{},...]",
        replyTo.path.address,
        different.size,
        different.keySet.take(3).mkString(",")
      )
      createGossipMessages2(different, sendBack = true, fromSystemUid).foreach { g =>
        replyTo ! g
      }
    }
  }

  def receiveStatus0(
    replyTo: ActorRef,
    thatDigests: Map[KeyId, Digest],
    fromSystemUid: Option[Long]
  ): Unit = {
    log.debug(
      "Received gossip status from {}, containing {} keys [{},...]",
      replyTo.path.address,
      thatDigests.keys.size,
      thatDigests.keys.take(5).mkString(", ")
    )

    def digestStatus(
      localDigest: Array[Byte],
      otherDigest: Array[Byte]
    ): DDataReplicator.DigestStatus = {
      val notFound = java.util.Arrays.equals(localDigest, DDataReplicator.NotFoundDig)
      if (!notFound && !java.util.Arrays.equals(localDigest, otherDigest)) DDataReplicator.DigestStatus.Different
      else if (notFound) DDataReplicator.DigestStatus.NotFound
      else DDataReplicator.DigestStatus.Same
    }

    val missingKeys                                  = new ArrayBuffer[String]()
    val different: mutable.Map[String, DataEnvelope] = new mutable.HashMap()
    thatDigests.foreach { case (key, thatDigest) =>
      getByKey(vehicleId2Long(key)) match {
        case Some((env, localDigest)) =>
          digestStatus(localDigest, thatDigest.toArrayUnsafe()) match {
            case DigestStatus.Same =>
            case DigestStatus.Different =>
              different.put(key, env)
            case DigestStatus.NotFound =>
              missingKeys += key
          }
        case None =>
          missingKeys += key
      }
    }

    if (different.nonEmpty) {
      log.warning(
        "Sending gossip to {}, containing {} keys [{},...]",
        replyTo.path.address,
        different.size,
        different.keySet.take(3).mkString(",")
      )
      createGossipMessages2(different, sendBack = true, fromSystemUid).foreach { g =>
        replyTo ! g
      }
    }

    if (missingKeys.nonEmpty) {
      log.warning("Requesting missing [{}]", missingKeys.mkString(", "))
      val status = Status(missingKeys.map(k => k -> NotFoundDigest).toMap, 0, 0, fromSystemUid, selfFromSystemUid)
      replyTo ! status
    }
  }

  def createGossipMessages2(
    different: mutable.Map[String, DataEnvelope],
    sendBack: Boolean,
    fromSystemUid: Option[Long]
  ): Vector[Gossip] = {
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

    different.foreach { case (key, dataEnvelope) =>
      val keySize = key.length + 4
      val dataSize = payloadSizeAggregator.getMaxSize(key) match {
        case 0    => payloadSizeAggregator.getMaxSize(key)
        case size => size
      }
      val envelopeSize = 100 + dataEnvelope.estimatedSizeWithoutData
      val entrySize    = keySize + dataSize + envelopeSize

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
    log.warning("Created [{}] Gossip messages from [{}] data entries.", messages.size, different.size)
    messages
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
        case 0    => payloadSizeAggregator.getMaxSize(key)
        case size => size
      }

      getData(vehicleId2Long(key)).foreach { dataEnvelope =>
        val envelopeSize = 100 + dataEnvelope.estimatedSizeWithoutData
        val entrySize    = keySize + dataSize + envelopeSize
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
    }

    addGossip()
    log.warning("Created [{}] Gossip messages from [{}] data entries.", messages.size, keys.size)
    messages
  }

  def receiveGossip(
    replyTo: ActorRef,
    updatedData: Map[KeyId, DataEnvelope],
    sendBack: Boolean,
    fromSystemUid: Option[Long]
  ): Unit = {
    // val replyTo = sender()
    log.warning(
      "Received gossip from [{}], containing {} keys [{}].",
      replyTo.path.address,
      updatedData.keySet.size,
      updatedData.keySet.take(5).mkString(", ")
    )

    var replyKeys    = Set.empty[KeyId]
    var gossipedKeys = Set.empty[Long]

    val writeBatch = new WriteBatch()
    updatedData.foreach { case (strKey, thatEnvelope) =>
      val key = vehicleId2Long(strKey)
      gossipedKeys += key
      getData(key) match {
        case Some(localEnv) =>
          if (localEnv != thatEnvelope) {
            val merged = (thatEnvelope.addSelf(selfUniqueAddress) merge localEnv).addSeen(selfAddress)
            writeBatch.put(columnFamily, Longs.toByteArray(key), serializer.toBinary(merged))
            if (merged.pruning.nonEmpty)
              replyKeys += strKey
          }
        case None =>
          val merged = thatEnvelope.addSelf(selfUniqueAddress).addSeen(selfAddress)
          writeBatch.put(columnFamily, Longs.toByteArray(key), serializer.toBinary(merged))
      }
    }
    if (gossipedKeys.nonEmpty)
      bf.tell(BFCmd.AddKeys(gossipedKeys))

    if (writeBatch.hasPut) {
      db.write(writeOptions, writeBatch)
      writeBatch.close()
    }

    if (sendBack && replyKeys.nonEmpty) {
      createGossipMessages(replyKeys.iterator, sendBack = false, fromSystemUid).foreach { gossip =>
        replyTo ! gossip
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

  def receiveRemovedNodePruningTick(): Unit =
    if (unreachable.isEmpty) {
      if (isLeader) {
        // Up and Removed members addresses the leader knows about.
        val knownNodes = allNodes.union(removedNodes.keySet)
        log.info("*** PruningRoundStarted: KnownNodes([{}])", knownNodes.mkString(","))
        collectRemovedNodes(knownNodes, self)(context.system.dispatchers.lookup(dbDispatcher))
      }
    }
}
