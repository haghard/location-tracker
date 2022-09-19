package com.rides

import com.typesafe.config.ConfigFactory

import java.net.{InetAddress, NetworkInterface}
import scala.io.Source
import scala.jdk.CollectionConverters.EnumerationHasAsScala
import scala.util.Try
import scala.util.Using

trait Ops {
  val Opt          = """(\S+)=(\S+)""".r
  val ethName      = "eth0"
  val ipExpression = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}"""

  def internalDockerAddr: Option[InetAddress] =
    NetworkInterface.getNetworkInterfaces.asScala.toList
      .find(_.getName == ethName)
      .flatMap(x => x.getInetAddresses.asScala.toList.find(i => i.getHostAddress.matches(ipExpression)))

  def argsToOpts(args: Seq[String]): Map[String, String] =
    args.collect { case Opt(key, value) => key -> value }.toMap

  def applySystemProperties(options: Map[String, String]): Unit =
    for ((key, value) <- options if key.startsWith("-D")) {
      val k = key.substring(2)
      println(s"Set $k: $value")
      System.setProperty(key.substring(2), value)
    }

  def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  def hostNameConfig(hostName: String) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.hostname = $hostName")

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  def readFile(path: String): Try[String] =
    Try(Source.fromFile(path)).map { src =>
      Using.resource(src) { reader =>
        val buffer = new StringBuffer()
        val iter   = reader.getLines
        while (iter.hasNext) buffer.append(iter.next)
        buffer.toString
      }
    }
}
