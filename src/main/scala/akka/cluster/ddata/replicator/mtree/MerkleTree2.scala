package akka.cluster.ddata.replicator

trait Node[T] {
  val hash: T
}

final case class Leaf[T](datum: T)(implicit hashFn: (T, Option[T]) => T) extends Node[T] {
  override val hash: T =
    hashFn(datum, None)
}

final case class Branch[T](left: Node[T], right: Option[Node[T]])(implicit hashFn: (T, Option[T]) => T)
    extends Node[T] {
  override val hash: T =
    hashFn(left.hash, right.flatMap(r => Some(r.hash)))
}

final case class MerkleTree2[T](val root: Node[T])

//https://www.works-hub.com/learn/grokking-merkle-tree-bccc4
object MerkleTree2 {

  def apply[T](data: Seq[T])(implicit hashFn: (T, Option[T]) => T): MerkleTree2[T] = {
    val withLeaves = data.map(Leaf(_))
    build(withLeaves)
  }

  def build[T](nodes: Seq[Node[T]])(implicit hashFn: (T, Option[T]) => T): MerkleTree2[T] =
    if (nodes.length == 1)
      MerkleTree2[T](nodes.head)
    else {
      val withBranches = nodes
        .grouped(2)
        .map {
          case Seq(a, b) => Branch(a, Some(b))
          case Seq(a)    => Branch(a, None)
        }
        .toSeq

      build(withBranches)
    }
}
