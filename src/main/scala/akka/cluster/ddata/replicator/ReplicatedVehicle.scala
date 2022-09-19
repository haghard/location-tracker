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

  def Key(id: String): Key[ReplicatedVehicle] = ReplicatedOrderKey(id)

  final case class ReplicatedOrderKey(_id: String) extends Key[ReplicatedVehicle](_id) with ReplicatedDataSerialization
}

final case class ReplicatedVehicle(
  state: VehicleStatePB,
  version: Long = 0L,                                    // version only moves forward
  replicationState: Map[UniqueAddress, Long] = Map.empty // Which replicas have seen which piece of information
) extends akka.cluster.ddata.ReplicatedData
    with ReplicatedDataSerialization
    with RemovedNodePruning { self =>

  type T = ReplicatedVehicle

  def update(order: VehicleStatePB, selfUniqueAddress: akka.cluster.UniqueAddress, revision: Long): T =
    copy(order, revision, self.replicationState.updated(selfUniqueAddress, revision))

  // isReplicated|isDurable
  def isDurable(
    clusterMembersView: Set[UniqueAddress],
    decider: ReplicatedVehicle.Decider = ReplicatedVehicle.atLeast(2)
  ): Boolean = {
    // Create a view so that when calculating the result we can break early once we've counted enough replicas.
    val votes: Iterable[Boolean] = clusterMembersView.view.map { m =>
      self.replicationState.get(m).getOrElse(-1L) == self.version
    }
    decider(votes)
  }

  def markSeen(
    selfMember: akka.cluster.UniqueAddress,
    that: Option[ReplicatedVehicle]
  ): ReplicatedVehicle =
    that match {
      case Some(other) =>
        copy(replicationState = replicationState.updated(selfMember, other.version.max(self.version))).merge(other)
      case None =>
        copy(replicationState = self.replicationState.updated(selfMember, self.version))
    }

  override def merge(that: ReplicatedVehicle): ReplicatedVehicle = {
    var merged = that.replicationState
    for ((key, thisValue) <- self.replicationState)
      merged.get(key) match {
        case Some(thatValue) =>
          val newValue = if (thisValue > thatValue) thisValue else thatValue
          if (newValue != thatValue)
            merged = merged.updated(key, newValue)
        case None =>
          merged = merged.updated(key, thisValue)
      }

    copy(if (self.version < that.version) that.state else self.state, self.version.max(that.version), merged)
  }

  override def modifiedByNodes: Set[UniqueAddress] = replicationState.keySet

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    replicationState.contains(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): T =
    replicationState.get(removedNode) match {
      case Some(cnt) =>
        val updated = (replicationState - removedNode) + (collapseInto -> cnt)
        copy(replicationState = updated)
      case None =>
        self
    }

  override def pruningCleanup(removedNode: UniqueAddress): T = {
    val updated = replicationState - removedNode
    // println(s"PruningCleanup:$removedNode")
    copy(replicationState = updated)
  }

  override def toString() =
    s"ReplVehicle(${state.toProtoString},$version,${replicationState.mkString(",")})"
}
