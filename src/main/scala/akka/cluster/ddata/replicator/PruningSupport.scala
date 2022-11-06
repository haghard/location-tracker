package akka.cluster.ddata.replicator

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.UniqueAddress
import akka.cluster.ddata.PruningState.{PruningInitialized, PruningPerformed}
import akka.cluster.ddata.RemovedNodePruning
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import one.nio.async.AsyncExecutor

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import scala.collection.SortedSet
import scala.concurrent.duration.FiniteDuration

object PruningSupport {
  case object PruningRoundFinished

  final case class StartPruning(removedNodes: scala.collection.Set[UniqueAddress])
  final case object PruningRoundInited

  sealed trait PruningAction

  object PruningAction {
    object Inited    extends PruningAction
    object Performed extends PruningAction
  }

  final case class PruningStep(key: String, envelope: DataEnvelope, removed: UniqueAddress, action: PruningAction)

  final case class DeleteObsoletePruningPerformed(key: String, envelope: DataEnvelope, removed: Set[UniqueAddress])

  final case class Param(removedSet: Set[UniqueAddress], key: String, envelope: java.lang.Object) // DataEnvelope

}

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
trait PruningSupport { _: Actor with ActorLogging =>

  def sharedMemoryMap: akka.cluster.ddata.durable.raf.SharedMemoryLongMap

  def selfUniqueAddress: UniqueAddress

  def pruningMarkerTTL: FiniteDuration

  // knownNodes == Up + Removed members addresses the leader knows about.
  def collectRemovedNodes(knownNodes: SortedSet[UniqueAddress]): Unit =
    sharedMemoryMap
      .collectRemovedNodesAsync { (addresses: scala.collection.Set[UniqueAddress], env: DataEnvelope) =>
        env match {
          case DataEnvelope(data: RemovedNodePruning, _, _) =>
            // data.modifiedByNodes.filter(n => n != selfUniqueAddress && !knownNodes.contains(n))
            data.modifiedByNodes.filterNot(n => n == selfUniqueAddress || knownNodes(n))
          case _ =>
            addresses
        }
      }
      .thenApply { removedAddresses =>
        log.warning(s"Unknown members [${removedAddresses.mkString(",")}] !")
        self ! PruningSupport.StartPruning(removedAddresses)
      } // TODO: what it addresses.size is too big ???

  def initPruning(removedSet: scala.collection.immutable.Set[UniqueAddress]): Unit = {

    def init(
      key: String,
      envelope: DataEnvelope,
      removed: UniqueAddress,
      selfAddress: UniqueAddress,
      replyTo: ActorRef
    ): Unit = {
      // Put a `PruningInitialized(selfAddress)` marker in the data envelope.
      val pruningInitializedEnvelop = envelope.initRemovedNodePruning(removed, selfAddress)
      replyTo ! PruningSupport.PruningStep(key, pruningInitializedEnvelop, removed, PruningSupport.PruningAction.Inited)
    }

    log.warning("Pruning:Step 1. Init [{}]", removedSet.mkString(","))
    val initCnt   = new AtomicLong(0L)
    val reInitCnt = new AtomicLong(0L)

    if (removedSet.nonEmpty) {
      sharedMemoryMap
        .initPruningAsync[DataEnvelope](
          removedSet,
          { p: PruningSupport.Param =>
            val envelope: DataEnvelope = p.envelope.asInstanceOf[DataEnvelope]
            p.removedSet.foreach { removed =>
              if (envelope.needPruningFrom(removed)) {
                envelope.data match {
                  case _: RemovedNodePruning =>
                    envelope.pruning.get(removed) match {
                      case None =>
                        initCnt.incrementAndGet()
                        init(p.key, envelope, removed, selfUniqueAddress, self)
                      case Some(PruningInitialized(owner, _)) if owner != selfUniqueAddress =>
                        // re-init pruning on this member
                        // log.warning("Re-init pruning: s:{} | o:{} | r:{} ", selfUniqueAddress, owner, removed)
                        reInitCnt.incrementAndGet()
                        init(p.key, envelope, removed, selfUniqueAddress, self)
                      case Some(PruningInitialized(owner, _)) =>
                      // already been initialized, ignore it
                    }

                  case _ =>
                }
              }
            }
          }
        )
        .thenApply { _ =>
          log.warning("Pruning:Step 1. [Initialized] = [init:{}:re-init:{}]", initCnt.get(), reInitCnt.get())
          self ! PruningSupport.PruningRoundInited
        }
    } else {
      log.warning("Pruning:Step 1. [Initialized] = [init:0:re-init:0]")
      self ! PruningSupport.PruningRoundInited
    }
  }

  def performPruning(allMembers: Set[UniqueAddress]): Unit = {
    // How long to keep PruningPerformed marker so that it protects us from zombies(resurrected data)
    val pruningPerformed = PruningPerformed(System.currentTimeMillis() + pruningMarkerTTL.toMillis)
    val cntr             = new AtomicLong(0L)
    sharedMemoryMap
      .performPruningAsync[DataEnvelope] { (key: String, envelope: DataEnvelope) =>
        envelope match {
          case DataEnvelope(_: RemovedNodePruning, pruning, _) =>
            pruning.foreach {
              case (removedAddress, PruningInitialized(pruningInitiator, seen)) =>
                // When the leader sees that all up nodes have seen the `PruningInitialized` marker (convergence on the PruningInitialized)
                // the leader performs the pruning and changes the marker to `PruningPerformed`, so that nobody else will redo the pruning.
                if (
                  pruningInitiator == selfUniqueAddress && (allMembers.isEmpty || allMembers
                    .forall(n => seen(n.address)))
                ) {
                  /*log.warning(
                    "*** PerformPruningAsync {}, {}, AllMembers[{}], Seen:[{}]",
                    key,
                    removedAddress,
                    allMembers,
                    seen.mkString(",")
                  )*/

                  cntr.incrementAndGet()
                  val pruned = envelope.prune(removedAddress, pruningPerformed)
                  self ! PruningSupport.PruningStep(key, pruned, removedAddress, PruningSupport.PruningAction.Performed)
                } else {
                  /*log.warning(
                    "PruningInitiator:{} Others:[{}] Seen:[{}] Bool:{}",
                    pruningInitiator,
                    allMembers.mkString(","),
                    seen.mkString(","),
                    allMembers.forall(n => seen(n.address))
                  )*/
                }
              case _ =>
            }

          case _ => // deleted, or pruning not needed
        }
      }
      .thenCompose { _ =>
        log.warning("Pruning:Step 2. [Perform] = {}", cntr.get())
        CompletableFuture.supplyAsync(() => deleteObsoletePruningMarkers(), AsyncExecutor.POOL)
      }
  }

  // Any node can run this
  def deleteObsoletePruningMarkers() = {
    val currentTime = System.currentTimeMillis()
    sharedMemoryMap
      .performPruningAsync[DataEnvelope] { (key: String, envelope: DataEnvelope) =>
        envelope match {
          case DataEnvelope(_: RemovedNodePruning, pruning, _) =>
            var removedAddresses = Set.empty[UniqueAddress]
            val newEnvelope = pruning.foldLeft(envelope) {
              case (acc, (removedAddress, p: PruningPerformed)) =>
                if (p.isObsolete(currentTime)) {
                  removedAddresses = removedAddresses + removedAddress
                  acc.copy(pruning = acc.pruning - removedAddress)
                } else acc
              case (acc, _) => acc
            }
            if (newEnvelope ne envelope) {
              self ! PruningSupport.DeleteObsoletePruningPerformed(key, newEnvelope, removedAddresses)
            }
        }
      }
      .thenApply(numOfKeys => self ! PruningSupport.PruningRoundFinished)
  }
}
