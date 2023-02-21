package akka.cluster.ddata.utils

import akka.util.ByteString
import org.bouncycastle.util.encoders.Hex as BHex

import scala.reflect.macros.blackbox
import scala.util.Try

//https://github.com/alephium/alephium/blob/master/util/src/main/scala/org/alephium/util/Hex.scala
object Hex {

  def unsafe(s: String): ByteString =
    ByteString.fromArrayUnsafe(BHex.decode(s))

  def from(s: String): Option[ByteString] =
    Try(unsafe(s)).toOption

  def toHexString(input: Array[Byte]): String =
    BHex.toHexString(input)

  def toHexString(input: IndexedSeq[Byte]): String =
    BHex.toHexString(input.toArray)

  // hex"aab64e9c814749cea508857b23c7550da30b67216950c461ccac1a14a58661c3"
  implicit class HexStringSyntax(val sc: StringContext) extends AnyVal {
    def hex(): ByteString = macro hexImpl
  }

  def hexImpl(c: blackbox.Context)(): c.Expr[ByteString] = {
    import c.universe._
    c.prefix.tree match {
      case Apply(_, List(Apply(_, List(Literal(Constant(s: String)))))) =>
        val bs = BHex.decode(s)
        c.Expr(q"akka.util.ByteString($bs)")
    }
  }
}
