package akka.cluster.ddata.replicator.bfs

import akka.cluster.ddata.replicator.bf.InvertibleBloomFilter

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

final case class Cell(count: Int, idSum: Long, hashSum: Int) { self =>

  def add(id: Int, idHashValue: Int): Cell =
    self.copy(count = count + 1, idSum = idSum ^ id, hashSum = hashSum ^ idHashValue)

  def isPure(digestFunction: MessageDigest): Boolean =
    (count == -1 || count == 1)
      && (InvertibleBloomFilter.genIdHash(
        String.valueOf(idSum).getBytes(StandardCharsets.UTF_8),
        digestFunction
      ) == hashSum)

}
