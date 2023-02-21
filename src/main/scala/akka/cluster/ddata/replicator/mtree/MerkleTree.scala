/*
package akka.cluster.ddata.replicator

import scala.annotation.tailrec
import scala.concurrent.Future

final case class Digest(hash: Array[Byte]) extends AnyVal { self =>

  def ++(that: Digest) = Digest(self.hash ++ that.hash)

  /*private def hex2bytes(hex: String): Array[Byte] =
    hex
      .replaceAll("[^0-9A-Fa-f]", "")
      .sliding(2, 2)
      .toArray
      .map(Integer.parseInt(_, 16).toByte)*/

  private def bytes2Hex(bytes: Array[Byte]): String = {
    val sb = new StringBuilder
    bytes.foreach(b => sb.append(String.format("%02X", b: java.lang.Byte)))
    sb.toString
  }

  override def toString: String = bytes2Hex(self.hash)
}

final case class NodeId(v: Int) extends AnyVal

trait MerkleDigest[T] {
  def digest(t: T): Digest
}

object MerkleDigest {

  type Block = Array[Byte]

  implicit object CRC32 extends MerkleDigest[Block] {
    override def digest(bytes: Block): Digest = {
      val digest = new java.util.zip.CRC32()
      digest.update(bytes)
      val bb = java.nio.ByteBuffer.allocate(8)
      bb.putLong(digest.getValue)
      Digest(bb.array)
    }
  }

  implicit object MD5 extends MerkleDigest[Block] {
    override def digest(bytes: Block): Digest =
      Digest(java.security.MessageDigest.getInstance("MD5").digest(bytes))
  }

  implicit object SHA1 extends MerkleDigest[Block] {
    override def digest(bytes: Block): Digest =
      Digest(java.security.MessageDigest.getInstance("SHA-1").digest(bytes))
  }

  implicit object SHA512 extends MerkleDigest[Block] {
    override def digest(bytes: Block): Digest =
      Digest(java.security.MessageDigest.getInstance("SHA-512").digest(bytes))
  }

  // https://www.pranaybathini.com/2021/05/merkle-tree.html
  implicit object Keccak256 extends MerkleDigest[Block] {
    override def digest(bytes: Block): Digest =
      Digest(
        org.bouncycastle.util.encoders.Hex
          .encode(new org.bouncycastle.jcajce.provider.digest.Keccak.Digest256().digest(bytes))
      )
  }
  // val dig = ByteString.fromArray(MessageDigest.getInstance("SHA-1").digest(bytes))
}

sealed trait MerkleTreeView {
  def digest: Digest
}

final case class Node(digest: Digest, left: MerkleTreeView, right: MerkleTreeView) extends MerkleTreeView
final case class Leaf(digest: Digest)                                              extends MerkleTreeView

sealed trait MerkleTree {
  def nodeId: NodeId

  def digest: Digest
}

final case class TreeNode(nodeId: NodeId, digest: Digest, left: MerkleTree, right: MerkleTree) extends MerkleTree

final case class TreeLeaf(nodeId: NodeId, digest: Digest) extends MerkleTree

//https://speedcom.github.io/dsp2017/2017/05/14/justindb-active-anti-entropy.html
//https://github.com/justin-db/JustinDB/blob/844a3f6f03192ff3e8248a15712fecd754e06fbc/justin-core/src/main/scala/justin/db/merkletrees/MerkleTree.scala
object MerkleTree {

  import MerkleDigest.Block
  // import MerkleDigest.Keccak256

  def fromArrays(blocks: Seq[Block])(implicit ev: MerkleDigest[Block]): MerkleTreeView =
    fromArray(blocks.toArray)

  def fromArray(blocks: Array[Block])(implicit dMaker: MerkleDigest[Block]): MerkleTreeView = { // MerkleTree

    def blockToLeaf(b: Block): MerkleTreeView = Leaf(dMaker.digest(b))

    def buildTree(blocks: Array[Block]): MerkleTreeView = {
      val leafs                     = blocks.map(blockToLeaf)
      var tree: Seq[MerkleTreeView] = leafs.toSeq
      // println("Size:" + tree.size)
      while (tree.length > 1)
        tree = tree.grouped(2).map(x => merge(x(0), x(1))).toSeq

      tree.head
    }

    def merge(n1: MerkleTreeView, n2: MerkleTreeView): MerkleTreeView = {
      val hash = dMaker.digest((n1.digest ++ n2.digest).hash)
      Node(hash, n1, n2)
    }

    def toFinalForm(tmt: MerkleTreeView): MerkleTree = {
      var counter = -1

      def toMerkle(mt: MerkleTreeView): MerkleTree = {
        counter += 1
        mt match {
          case Leaf(digest)              => TreeLeaf(NodeId(counter), digest)
          case Node(digest, left, right) => TreeNode(NodeId(counter), digest, toMerkle(left), toMerkle(right))
        }
      }

      toMerkle(tmt)
    }

    val z = zeroed(blocks)
    // println("Len:" + blocks.length + " Zerod:" + z.length)
    val t = buildTree(blocks ++ z) // .toOption
    // .map(toFinalForm)
    // toFinalForm(t)
    t
  }

  def zeroed(blocks: Seq[Block]): Array[Array[Byte]] = {

    def nextPowerOfTwo(value: Int): Int = 1 << (32 - Integer.numberOfLeadingZeros(value - 1))

    def zero(i: Int): Int =
      nextPowerOfTwo(i) - i

    /*val factor = 2
      var x      = factor
      while (x < i) x *= factor
      x - i*/

    Array.fill(zero(blocks.length))(Array[Byte](0))
  }

  @tailrec def nodeById(nodeId: NodeId, merkleTree: MerkleTree): Option[MerkleTree] =
    if (merkleTree.nodeId == nodeId) Some(merkleTree)
    else
      merkleTree match {
        case TreeNode(_, _, left, right) =>
          if (nodeId.v >= right.nodeId.v) nodeById(nodeId, right) else nodeById(nodeId, left)
        case _ => None
      }
}

//runMain akka.cluster.ddata.replicator.Runner
object Runner extends App {
  // Seq[Block]

  val blocks = Array(
    Array[Byte](1, 2, 3),
    Array[Byte](4, 5, 6),
    Array[Byte](7, 8, 9),
    Array[Byte](10, 11, 12),
    Array[Byte](20, 21, 22),
    Array[Byte](30, 31, 32),
    Array[Byte](40, 41, 42)
  )

  val blocks2 = Array(
    Array[Byte](1, 2, 3),
    Array[Byte](4, 5, 6),
    Array[Byte](7, 8, 9),
    Array[Byte](10, 11, 12),
    Array[Byte](20, 21, 22),
    Array[Byte](30, 31, 32),
    Array[Byte](40, 41, 42)
  )

  val blocks3 = Array(
    Array[Byte](1, 2, 3),
    Array[Byte](4, 5, 6),
    Array[Byte](7, 8, 9),
    Array[Byte](10, 11, 12)
  )

  // MerkleTree.fromArrays(blocks3)(MerkleDigest.MD5)

  /*
    4 keys means the tree with contain 6 hashes

                0
        +----------------+
        |                |
     -- 1 --          -- 4 --
   2(h1)| 3(h2)     5(h3)  6(h4)

 */

  MerkleTree.fromArrays(blocks)(MerkleDigest.SHA1)

  // SHA1:AE47B8AE400738EBF225A9A2E3A7D0A8DEE0D558
  // MD5: D7A7BC22E9F3F0FF07100DC8B4D3DC14
  // CRC32: 000000000B73036C
  val tree1 = MerkleTree.fromArrays(blocks)(MerkleDigest.SHA512)
  val tree2 = MerkleTree.fromArrays(blocks2)(MerkleDigest.SHA512)
  println(tree1.digest.toString)
  println(tree2.digest.toString)

  /*
  val one = NodeId(1)
  val a   = MerkleTree.nodeById(one, tree1).get.digest
  val b   = MerkleTree.nodeById(one, tree2).get.digest

  println(a.toString)
  println(b.toString)
  println(java.util.Arrays.equals(a.hash, b.hash))

  val two = NodeId(6)
  val c = MerkleTree.nodeById(two, tree1).get.digest
  val d = MerkleTree.nodeById(two, tree2).get.digest
  println(java.util.Arrays.equals(c.hash, d.hash))


  val digest1 = tree1.digest
  val digest2 = tree2.digest

  //println(digest1.hash.mkString(","))

  //If the hash values of the root of two trees are equal, then its meaning that leaf nodes are equal
  //(there is no point in doing synchronization since)

  digest1.hash sameElements digest2.hash
  val r = java.util.Arrays.equals(digest1.hash, digest2.hash)
  println(r)
 */
}
 */

/*
it should "have the same top hash" in {
    val blocks: Seq[Block] = Seq(
      Array[Byte](1,2,3),
      Array[Byte](4,5,6),
      Array[Byte](7,8,9),
      Array[Byte](10,11,12)
    )
    val blocks2: Seq[Block] = Seq(
      Array[Byte](1,2,3),
      Array[Byte](4,5,6),
      Array[Byte](7,8,9),
      Array[Byte](10,11,12)
    )

    val digest1 = MerkleTree.unapply(blocks)(MerkleDigest.CRC32).get.digest
    val digest2 = MerkleTree.unapply(blocks2)(MerkleDigest.CRC32).get.digest

    digest1.hash.deep shouldBe digest2.hash.deep
  }


https://www.works-hub.com/learn/grokking-merkle-tree-bccc4
https://github.com/ferrlin/grokking-merkle-tree.git
https://www.linkedin.com/pulse/modern-data-structure-merkle-tree-abhilash-krishnan/
https://www.pranaybathini.com/2021/05/merkle-tree.html



https://medium.com/@nishantparmar/distributed-system-design-patterns-2d20908fecfc

19. Merkle Trees
Read Repair removes conflicts while serving read requests. But, if a replica falls significantly behind others, it might take a very long time to resolve conflicts.
A replica can contain a lot of data. Naively splitting up the entire range to calculate checksums for comparison is not very feasible; there is simply too much data to be transferred.
Instead, we can use Merkle trees to compare replicas of a range.
A Merkle tree is a binary tree of hashes, where each internal node is the hash of its two children, and each leaf node is a hash of a portion of the original data.

Comparing Merkle trees is conceptually simple:
Compare the root hashes of both trees.
 1. If they are equal, stop.
 2. Recurse on the left and right children.
 3. For anti-entropy and to resolve conflicts in the background, Dynamo uses Merkle trees.
 */
