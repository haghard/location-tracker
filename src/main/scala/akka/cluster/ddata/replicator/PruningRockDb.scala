package akka.cluster.ddata.replicator

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.cluster.UniqueAddress
import akka.cluster.ddata.PruningState.PruningInitialized
import akka.cluster.ddata.PruningState.PruningPerformed
import akka.cluster.ddata.RemovedNodePruning
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.cluster.ddata.replicator.DDataReplicatorRocksDB.DataEnvelopeManifest
import akka.serialization.SerializerWithStringManifest
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.ReadOptions
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions

import java.nio.charset.StandardCharsets
import java.util.concurrent.ThreadLocalRandom
import scala.collection.SortedSet
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

// format: off
/** Leaders actions are only allowed if there is convergence to make sure that state changes are originated from a
  * single source.
  *
  * See 'CRDT Garbage' section in Replicator Scaladoc for description of the process
  *
  * The pruning consists of several steps:
  *
  * 0. When a node is removed from the cluster it is first important that all updates that were done by that node are
  * disseminated to all other nodes. The pruning will not start before the `maxPruningDissemination` duration has
  * elapsed. The time measurement is stopped when any replica is unreachable, but it's still recommended to configure
  * this with certain margin (As long as we have unreachable members the pruning stays blocked). It should be in the
  * magnitude of minutes.
  *
  * 1. The leader initiates the pruning by adding a [[PruningInitialized]] marker in the data envelope. This is gossiped
  * to all other members and they mark it as seen when they receive it.
  *
  * 2. When the leader sees that all other nodes have seen the `PruningInitialized` marker (convergence on the
  * PruningInitialized what means that state change is originated from the leader) the leader performs the pruning and
  * changes the marker to `PruningPerformed` so that nobody else will redo the pruning.
  *
  * The data envelope with this pruning state is a CRDT itself. The pruning is typically performed by "moving" the part
  * of the data associated with the removed node to the leader node. For example, a `GCounter` is a `Map` with the node
  * as key and the counts done by that node as value. When pruning the value of the removed node is moved to the entry
  * owned by the leader node. See [[RemovedNodePruning#prune]] (When the `removed` node has been removed from the
  * cluster the state changes from that node will be pruned by collapsing the data entries to another node)
  *
  * 3. Thereafter the data is always cleared from parts associated with the removed node so that it does not come back
  * when merging. See [[RemovedNodePruning#pruningCleanup]]
  *
  * 4. After another `maxPruningDissemination` period, after pruning the last entry from the removed node the
  * `PruningPerformed` markers in the data envelope are collapsed into a single tombstone entry, for efficiency. Clients
  * may continue to use old data and therefore all data are always cleared from parts associated with tombstones nodes.
  *
  * TODO:
  *
  * a) Currently all methods scan over all keys each time. We need to scan using fixed ranges to avoid memory hit.
  * b) Replace self with a separate actor that is responsible only for pruning.
  */
// format: on
trait PruningRockDb {

  _: Actor with ActorLogging =>

  def selfUniqueAddress: UniqueAddress

  def pruningMarkerTTL: FiniteDuration

  def serializer: SerializerWithStringManifest

  def gossipMaxDeltaElements: Int

  def db: RocksDB
  def writeOptions: WriteOptions
  def columnFamily: ColumnFamilyHandle

  def collectRemovedNodes(
    knownNodes: SortedSet[UniqueAddress],
    replyTo: ActorRef
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      val limit             = 6 // TODO: Config
      var addressesToRemove = Set.empty[UniqueAddress]

      // Readers do not block writers.
      val snapshot = db.getSnapshot
      val readOps  = new ReadOptions().setSnapshot(snapshot)
      val iter     = db.newIterator(columnFamily, readOps)
      try {
        iter.seekToFirst()
        while (iter.isValid && addressesToRemove.size <= limit) {
          val envelope = iter.value()
          serializer.fromBinary(envelope, DataEnvelopeManifest).asInstanceOf[DataEnvelope] match {
            case DataEnvelope(data: RemovedNodePruning, _, _) =>
              val removed = data.modifiedByNodes.filterNot(n => n == selfUniqueAddress || knownNodes(n))
              addressesToRemove = addressesToRemove ++ removed
            case _ =>
          }
          iter.next()
        }
      } finally
        try iter.close()
        finally
          try readOps.close()
          finally
            db.releaseSnapshot(snapshot)

      log.warning(s"Members to remove:${addressesToRemove.size} - [${addressesToRemove.mkString(",")}]")
      replyTo ! PruningSupport.StartPruning(addressesToRemove)
    }

  /** The leader initiates the pruning by adding a [[PruningInitialized]] marker in the data envelope. This is gossiped
    * to all other members and they mark it as seen when they receive it.
    */
  def initPruning(
    nodesToRemove: scala.collection.immutable.Set[UniqueAddress]
  ): mutable.Set[Array[Byte]] = {
    var init       = 0
    var reinit     = 0
    var inProgress = 0
    var i          = gossipMaxDeltaElements
    val iter       = db.newIterator(columnFamily)

    val keysToPrune = mutable.Set.empty[Array[Byte]]
    val writeBatch  = new WriteBatch()
    try {
      iter.seekToFirst()
      while (iter.isValid && i > 0) {
        val key      = iter.key()
        val envelope = iter.value()

        val env = serializer.fromBinary(envelope, DataEnvelopeManifest).asInstanceOf[DataEnvelope]
        nodesToRemove.foreach { rmv =>
          if (env.needPruningFrom(rmv)) {
            // val strKey = new String(key, StandardCharsets.UTF_8)
            env.data match {
              case _: RemovedNodePruning =>
                env.pruning.get(rmv) match {
                  case None =>
                    // log.warning(s"InitPruning $strKey:$rmv owner:$selfUniqueAddress")
                    init = init + 1
                    keysToPrune.+=(key)
                    val initialized = env.initRemovedNodePruning(rmv, selfUniqueAddress)
                    writeBatch.put(columnFamily, key, serializer.toBinary(initialized))
                    i = i - 1

                  case Some(PruningInitialized(owner, _)) if owner != selfUniqueAddress =>
                    // log.warning(s"ReInitPruning($strKey:$rmv Owner:$owner, SelfUniqueAddress:$selfUniqueAddress")
                    reinit = reinit + 1
                    keysToPrune.+=(key)
                    val initialized = env.initRemovedNodePruning(rmv, selfUniqueAddress)
                    writeBatch.put(columnFamily, key, serializer.toBinary(initialized))
                    i = i - 1

                  case Some(PruningInitialized(owner, _)) =>
                    // other owner
                    keysToPrune.+=(key)
                    inProgress = inProgress + 1
                }
              case _ =>
            }
          }
        }
        iter.next()
      }
    } finally
      iter.close()

    if (writeBatch.hasPut) {
      db.write(writeOptions, writeBatch)
      writeBatch.close()
    }

    log.info(
      "InitPruning.Step1 [Initialized: init:{}/re-init:{}/in-progress:{}]",
      init,
      reinit,
      inProgress
    )
    keysToPrune.result()
  }

  def performPruning(
    all: Set[UniqueAddress],
    keysToPrune: mutable.Set[Array[Byte]]
  ): mutable.Set[Array[Byte]] = {
    val pruningPerformed = PruningPerformed(System.currentTimeMillis() + pruningMarkerTTL.toMillis)
    val keysToCleanup    = mutable.Set.empty[Array[Byte]]

    val writeBatch = new WriteBatch()
    keysToPrune.foreach { keyBts =>
      val key      = new String(keyBts, StandardCharsets.UTF_8)
      val envelope = db.get(columnFamily, keyBts)

      serializer.fromBinary(envelope, DataEnvelopeManifest).asInstanceOf[DataEnvelope] match {
        case e @ DataEnvelope(_: RemovedNodePruning, pruning, _) =>
          pruning.foreach {
            case (removedAddress, PruningInitialized(initiator, seen)) =>
              // When the leader sees that all up nodes have seen the `PruningInitialized` marker (convergence on the PruningInitialized)
              // the leader performs the pruning and changes the marker to `PruningPerformed`, so that nobody else will redo the pruning.
              val AllAdds = all.map(_.address)
              if (initiator == selfUniqueAddress && (all.isEmpty || seen.intersect(AllAdds) == AllAdds)) {
                if (ThreadLocalRandom.current().nextDouble() > .9)
                  log.warning("***** Pruned:{}. Key:{} Seen:[{}]", removedAddress, key, seen.mkString(","))

                val pruned = e.prune(removedAddress, pruningPerformed)
                writeBatch.put(columnFamily, keyBts, serializer.toBinary(pruned))
                keysToCleanup.+=(keyBts)
              } else {
                /*log.warning(
                  "Not all UP nodes have seen the `Inited` for key: {} Seen:[{}] ({}vs{})",
                  key,
                  seen.mkString(","),
                  selfUniqueAddress,
                  initiator
                )*/
              }
            case _ =>
          }

        case _ => // deleted, or pruning not needed
      }
    }

    if (writeBatch.hasPut) {
      db.write(writeOptions, writeBatch)
      writeBatch.close()
    }

    keysToCleanup.result()
  }

  def deleteObsoletePruningMarkers(
    keysToDeleteMarker: mutable.Set[Array[Byte]]
  ): Set[UniqueAddress] = {
    val now                      = System.currentTimeMillis()
    var adds: Set[UniqueAddress] = Set.empty

    val writeBatch = new WriteBatch()
    keysToDeleteMarker.foreach { keyBts =>
      val envelope = db.get(columnFamily, keyBts)
      serializer.fromBinary(envelope, DataEnvelopeManifest).asInstanceOf[DataEnvelope] match {
        case currentEnv @ DataEnvelope(_: RemovedNodePruning, pruning, _) =>
          var removedAddresses = Set.empty[UniqueAddress]
          val newEnvelope = pruning.foldLeft(currentEnv) {
            case (acc, (removedAddress, p: PruningPerformed)) =>
              if (p.isObsolete(now)) {
                removedAddresses = removedAddresses + removedAddress
                acc.copy(pruning = acc.pruning - removedAddress)
              } else acc
            case (acc, _) =>
              acc
          }

          if (newEnvelope ne currentEnv) {
            writeBatch.put(columnFamily, keyBts, serializer.toBinary(newEnvelope))
            adds = adds ++ removedAddresses
          }
        case _ =>
      }
    }

    if (writeBatch.hasPut) {
      db.write(writeOptions, writeBatch)
      writeBatch.close()
    }

    adds
  }
}
