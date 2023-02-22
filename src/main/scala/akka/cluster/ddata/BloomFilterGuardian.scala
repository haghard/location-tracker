package akka.cluster.ddata

import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.Replicator.Internal.Digest
import akka.cluster.ddata.Replicator.Internal.Status
import akka.cluster.ddata.replicator.DDataReplicatorRocksDB
import akka.cluster.ddata.replicator.bf2.BloomFilterRange
import akka.stream.BoundedSourceQueue
import com.google.common.primitives.Longs
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.ReadOptions
import org.rocksdb.RocksDB

import scala.collection.immutable
import scala.collection.mutable

/*
  TODO:
    Mark ranges as dirty
    Use keysBitMap.forEachInRange() to create BloomFilterRange for dirty ranges.
 */
object BloomFilterGuardian {

  sealed trait BFCmd
  object BFCmd {
    final case class PutKey(key: Long) extends BFCmd
    final case class SendStatus(
      dest: ActorSelection,
      replicator: ActorRef,
      addressLongUid: Long,
      selfFromSystemUid: Option[Long]
    ) extends BFCmd

    final case class EvalDiff(otherBF: Digest, replyTo: ActorRef, fromSystemUid: Option[Long]) extends BFCmd

    final case class AddKeys(value: Set[Long]) extends BFCmd
  }

  def apply(
    db: RocksDB,
    cFamily: ColumnFamilyHandle,
    internals: BoundedSourceQueue[DDataReplicatorRocksDB.RMsg.Internal],
    maxGossipElements: Int
  ): Behavior[BFCmd] =
    Behaviors.setup { ctx =>
      val keysBitMap = load(db, cFamily)
      ctx.log.warn("★ ★ ★  KeysBitMap.Cardinality {}  ★ ★ ★ ", keysBitMap.getLongCardinality)
      active(keysBitMap, internals, maxGossipElements)
    }

  def active(
    keysBitMap: org.roaringbitmap.longlong.Roaring64Bitmap,
    internalChannel: BoundedSourceQueue[DDataReplicatorRocksDB.RMsg.Internal],
    maxGossipElements: Int
  ): Behavior[BFCmd] =
    Behaviors.receiveMessage {
      case BFCmd.PutKey(key) =>
        // TODO: collect all added keys here between
        keysBitMap.add(key)
        Behaviors.same

      case BFCmd.AddKeys(keys) =>
        keys.foreach(keysBitMap.add(_))
        Behaviors.same

      case BFCmd.SendStatus(dest, replicator, addressLongUid, selfFromSystemUid) =>
        val bf = BloomFilterRange[Long]()
        keysBitMap.forEach((value: Long) => bf.:+(value))

        val bts = BloomFilterRange.toByteArray(bf)
        val status = Status(
          immutable.Map
            .empty[String, Digest] + (DDataReplicatorRocksDB.BF_KEY -> akka.util.ByteString.fromArrayUnsafe(bts)),
          0,
          0,
          Some(addressLongUid),
          selfFromSystemUid
        )

        dest.tell(status, replicator)
        Behaviors.same

      case BFCmd.EvalDiff(otherBF, replyTo, fromSystemUid) =>
        val bf          = BloomFilterRange.fromBts[Long](otherBF.toArrayUnsafe())
        val unknownKeys = mutable.Set.empty[Long]

        var limit = maxGossipElements
        val iter  = keysBitMap.getLongIterator
        while (iter.hasNext && limit > 0) {
          val key = iter.next()
          if (!bf.mightContain(key)) {
            unknownKeys += key
            limit -= 1
          }
        }

        if (unknownKeys.nonEmpty)
          internalChannel.offer(
            DDataReplicatorRocksDB.RMsg.Internal(
              DDataReplicatorRocksDB.RInternal.KeysSetDiff(unknownKeys, replyTo, fromSystemUid)
            )
          )

        Behaviors.same
    }

  private def load(db: RocksDB, cFamily: ColumnFamilyHandle): org.roaringbitmap.longlong.Roaring64Bitmap = {
    val bm       = new org.roaringbitmap.longlong.Roaring64Bitmap()
    val snapshot = db.getSnapshot
    val readOps  = new ReadOptions().setSnapshot(snapshot)
    val iter     = db.newIterator(cFamily, readOps)
    try {
      iter.seekToFirst()
      while (iter.isValid) {
        val key = Longs.fromByteArray(iter.key())
        bm.add(key)
        iter.next()
      }
    } finally
      try iter.close()
      finally
        try readOps.close()
        finally
          db.releaseSnapshot(snapshot)

    bm
  }
}
