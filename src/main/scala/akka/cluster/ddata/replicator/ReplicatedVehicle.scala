package akka.cluster.ddata.replicator

import akka.cluster.UniqueAddress
import akka.cluster.ddata.{Key, RemovedNodePruning, ReplicatedDataSerialization}
import com.rides.domain.types.protobuf.VehicleStatePB

object ReplicatedVehicle {

  /** Decider for voting.
    */
  type Decider = Iterable[Boolean] => Boolean

  /** At least `n` nodes must vote positive.
    */
  def atLeast(n: Int): Decider = { iter =>
    // iter.count(identity) >= n
    iter
      .filter(identity)
      .take(n)
      .size >= n
  }
  // _.filter(identity).take(n).size >= n

  /** At least one node must vote positive.
    */
  val AtLeastOne: Decider = atLeast(1)

  /** At most `n` node must vote positive.
    */
  def atMost(n: Int): Decider = _.filter(identity).take(n + 1).size <= n

  /** All nodes must vote positive.
    */
  val All: Decider = _.forall(identity)

  /** A majority of nodes must vote positive
    */
  val Majority: Decider = { votes =>
    // votes.count(identity) > (votes.size / 2)

    val (totalVoters, votesFor) = votes.foldLeft((0, 0)) { case ((total, votes), vote) =>
      (total + 1, if (vote) votes + 1 else votes)
    }
    votesFor > totalVoters / 2
  }

  def Key(id: String): Key[ReplicatedVehicle] = ReplicatedVehicleKey(id)

  final case class ReplicatedVehicleKey(_id: String)
      extends Key[ReplicatedVehicle](_id)
      with ReplicatedDataSerialization
}

/** Each node sequentially numbers the updates that it generates, and so the set of updates that a node has delivered
  * can be summarised by remembering just the highest sequence number from each node.
  */
final case class ReplicatedVehicle(
  state: VehicleStatePB,
  // writerEpoch: Long, //https://martinfowler.com/articles/patterns-of-distributed-systems/generation.html
  version: Long = 0L,                                    // version only moves forward
  replicationState: Map[UniqueAddress, Long] = Map.empty // which replicas have seen which versions
) extends akka.cluster.ddata.ReplicatedData
    with ReplicatedDataSerialization
    with RemovedNodePruning { self =>

  type T = ReplicatedVehicle

  def update(vehicle: VehicleStatePB, selfUniqueAddress: akka.cluster.UniqueAddress, revision: Long): T =
    self.copy(vehicle, /*writerEpoch,*/ revision, self.replicationState.updated(selfUniqueAddress, revision))

  // isReplicated|isDurable
  def isDurable(
    clusterMembersView: Set[UniqueAddress],
    decider: ReplicatedVehicle.Decider = ReplicatedVehicle.atLeast(2)
  ): Boolean = {
    // Create a view so that when calculating the result we can break early once we've counted enough replicas.
    val votes: Iterable[Boolean] = clusterMembersView.view.map { uniqueAddress =>
      self.replicationState.get(uniqueAddress).getOrElse(-1L) == self.version
    }
    decider(votes)
  }

  def markSeen(selfMember: akka.cluster.UniqueAddress, that: Option[ReplicatedVehicle]): ReplicatedVehicle =
    that match {
      case Some(other) =>
        self.copy(replicationState = replicationState.updated(selfMember, other.version.max(self.version))).merge(other)
      case None =>
        self.copy(replicationState = replicationState.updated(selfMember, self.version))
    }

  override def merge(that: ReplicatedVehicle): ReplicatedVehicle = {
    var merged = that.replicationState
    for ((key, thisVersion) <- self.replicationState)
      merged.get(key) match {
        case Some(thatVersion) =>
          val version = if (thisVersion > thatVersion) thisVersion else thatVersion
          if (version != thatVersion)
            merged = merged.updated(key, version)
        case None =>
          merged = merged.updated(key, thisVersion)
      }

    self.copy(
      if (self.version < that.version) that.state else self.state,
      // self.writerEpoch,
      self.version.max(that.version),
      merged
    )
  }

  override def modifiedByNodes: Set[UniqueAddress] = replicationState.keySet

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    replicationState.contains(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): T =
    replicationState.get(removedNode) match {
      case Some(cnt) =>
        val updated = (replicationState - removedNode) + (collapseInto -> cnt)
        self.copy(replicationState = updated)
      case None =>
        self
    }

  override def pruningCleanup(removedNode: UniqueAddress): T = {
    val updated = replicationState - removedNode
    // println(s"PruningCleanup:$removedNode")
    self.copy(replicationState = updated)
  }

  override def toString() =
    s"RVehicle(${state.toProtoString},$version,${replicationState.mkString(",")})"
}
