package akka.cluster.ddata.replicator.bf2

import bloomfilter.CanGenerateHashFrom

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import scala.util.Using

object BloomFilterRange {

  def fromBts[T](bytes: Array[Byte])(implicit
    H: CanGenerateHashFrom[T]
  ): BloomFilterRange[T] =
    Using.resource(new ByteArrayInputStream(bytes)) { in =>
      apply(bloomfilter.mutable.BloomFilter.readFrom[T](in))
    }

  def toByteArray[T](filter: BloomFilterRange[T]): Array[Byte] =
    Using.resource(new ByteArrayOutputStream(filter.sizeBytes)) { out =>
      filter.bf.writeTo(out)
      out.toByteArray
    }

  def apply[A](
    expectedElements: Long = 50_000L, // 50 889
    falsePositiveRate: Double = 0.02
  )(implicit canBuildHash: CanGenerateHashFrom[A]): BloomFilterRange[A] =
    BloomFilterRange(bloomfilter.mutable.BloomFilter[A](expectedElements, falsePositiveRate))
}

final case class BloomFilterRange[A] private (
  private val bf: bloomfilter.mutable.BloomFilter[A]
) { self =>

  def :+(entityId: A): BloomFilterRange[A] = {
    self.bf.add(entityId)
    self
  }

  def mightContain(entityId: A): Boolean =
    self.bf.mightContain(entityId)

  def sizeBits: Long = self.bf.numberOfBits

  def sizeBytes: Int = (self.bf.numberOfBits / 8).toInt

  def merge(that: BloomFilterRange[A]): BloomFilterRange[A] =
    BloomFilterRange(self.bf.union(that.bf))

  override def toString: String = s"BFRange($sizeBytes bts)"
}
