package akka.cluster.ddata.utils

import org.bouncycastle.util.Arrays

import java.nio.charset.StandardCharsets
import scala.annotation.tailrec

final case class Digest(hash: Array[Byte]) extends AnyVal { self =>

  def ++(that: Digest) =
    Digest(Arrays.concatenate(self.hash, that.hash))
  // Digest(self.hash ++ that.hash)

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

  override def toString: String =
    new String(org.bouncycastle.util.encoders.Hex.encode(hash), StandardCharsets.UTF_8)
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

  implicit object SHA3_256 extends MerkleDigest[Block] {
    override def digest(bytes: Block): Digest =
      Digest(java.security.MessageDigest.getInstance("SHA3-256").digest(bytes))
  }

  implicit object SHA512 extends MerkleDigest[Block] {
    override def digest(bytes: Block): Digest =
      Digest(java.security.MessageDigest.getInstance("SHA-512").digest(bytes))
  }

  // https://www.pranaybathini.com/2021/05/merkle-tree.html
  implicit object Keccak256 extends MerkleDigest[Block] {
    override def digest(bytes: Block): Digest =
      Digest(new org.bouncycastle.jcajce.provider.digest.Keccak.Digest256().digest(bytes))
  }
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

    def buildTree(
      blocks: Array[Block]
    ): MerkleTreeView = {
      val leafs                     = blocks.map(blockToLeaf)
      var tree: Seq[MerkleTreeView] = leafs.toSeq
      println("Size:" + tree.size)

      while (tree.length > 1)
        tree = tree.grouped(2).map(x => merge(x(0), x(1))).toSeq

      tree.head
    }

    def merge(n1: MerkleTreeView, n2: MerkleTreeView): MerkleTreeView = {
      val hash = dMaker.digest((n1.digest ++ n2.digest).hash)
      // println(n1.digest.toString + " + " + n2.digest.toString + " = " + hash.toString)
      Node(hash, n1, n2)
    }

    val z = zeroed(blocks)
    buildTree(blocks ++ z)
  }

  def zeroed(blocks: Seq[Block]): Array[Array[Byte]] = {
    def nextPowerOfTwo(value: Int): Int = 1 << (32 - Integer.numberOfLeadingZeros(value - 1))

    def zero(i: Int): Int = nextPowerOfTwo(i) - i

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

//runMain akka.cluster.ddata.utils.Runner
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

  // SHA1:AE47B8AE400738EBF225A9A2E3A7D0A8DEE0D558
  // MD5: D7A7BC22E9F3F0FF07100DC8B4D3DC14
  // CRC32: 000000000B73036C
  // val tree1 = MerkleTree.fromArrays(blocks)(MerkleDigest.SHA512)
  // val tree2 = MerkleTree.fromArrays(blocks2)(MerkleDigest.SHA512)
  // println(tree1.digest.toString)
  // println(tree2.digest.toString)

  val testBlocks = Array(
    "Captain America".getBytes(StandardCharsets.UTF_8),
    "Iron Man".getBytes(StandardCharsets.UTF_8),
    "God of thunder".getBytes(StandardCharsets.UTF_8),
    "Doctor strange".getBytes(StandardCharsets.UTF_8),
    "dfadsfasgsdg sdfgdfg df g asgfDoctor strange".getBytes(StandardCharsets.UTF_8),
    "ertw34t fgsd sdfdfadsfasgsdg sdfgdfg df g asgfDoctor strange".getBytes(StandardCharsets.UTF_8),
    "11God of thunder".getBytes(StandardCharsets.UTF_8),
    "223Doctor strange".getBytes(StandardCharsets.UTF_8)
  )

  // 62c8100c3212eab6f63564fed7a4357e3a8e40bd2b6826daeca287fa2c6cb699
  // 2e19f92aa29a76b10a658330326804b7b53df3819918f114cc74edaa5a6b36d3
  // aad0ae628c37a74dd7cfff53616c88d24e69fa05e2698442fa738ec4e1d787686045fcb91790c9c6bb626a8592311658e71515bbd03321abfbbaa557f95b4830
  // SH1 d402ef930dd98626e629f01a89f75e906cfa6f26
  // f26761ea21ebb2e17455bdac4efd095a
  println(MerkleTree.fromArray(testBlocks)(MerkleDigest.Keccak256).digest.toString)

  println("**************************")
  println(
    akka.cluster.ddata.replicator.mtree.JMerkleTree
      .treeFromScala(
        testBlocks.asInstanceOf[Array[AnyRef]],
        (bts: Array[Byte]) => new org.bouncycastle.jcajce.provider.digest.Keccak.Digest256().digest(bts)
      )
      .getHashStr
  )

  // MerkleTree.fromArrays(testBlocks)(MerkleDigest.SHA512)

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
 */
