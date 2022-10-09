package akka.cluster
package ddata

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.ddata.MMapReader.Protocol.Tick
import akka.cluster.ddata.durable.raf.SharedMemoryLongMap

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object MMapReader {

  sealed trait Protocol
  object Protocol {
    final case class Completed(cnt: Int)                                               extends Protocol
    final case class KeyRange(offset: Int, keys: scala.collection.mutable.Set[String]) extends Protocol
    final case class Fail(ex: Throwable)                                               extends Protocol
    final case object Written                                                          extends Protocol
    final case object Tick                                                             extends Protocol
  }

  private def readRange(startIndex: Int, data: SharedMemoryLongMap, pageSize: Int)(implicit
    ec: ExecutionContext
  ): Future[Protocol.KeyRange] =
    scala.compat.java8.FutureConverters
      .toScala(data.collectKeysAsync(startIndex, pageSize))
      .map(env => Protocol.KeyRange(env.getOffset, env.getResults))

  private def mapResult[T <: Protocol]: Try[T] => Protocol = {
    case Success(reply) => reply
    case Failure(ex)    => Protocol.Fail(ex)
  }

  def apply(
    data: SharedMemoryLongMap,
    pageSize: Int = 1 << 4
  ): Behavior[Protocol] =
    Behaviors.setup { implicit ctx =>
      Behaviors.withTimers { t =>
        t.startSingleTimer(Tick, 15.seconds)
        idle(data, pageSize)
      }
    }

  def idle(
    data: SharedMemoryLongMap,
    pageSize: Int
  )(implicit ctx: ActorContext[Protocol]): Behavior[Protocol] =
    Behaviors.receiveMessagePartial { case Tick =>
      ctx.pipeToSelf(readRange(0, data, pageSize)(ctx.executionContext))(mapResult)
      reading(0, 0, 500.millis)(ctx, pageSize, data)
    }

  private def reading(prevOffset: Int, count: Int, delay: FiniteDuration)(implicit
    ctx: ActorContext[Protocol],
    pageSize: Int,
    data: SharedMemoryLongMap
  ): Behavior[Protocol] =
    Behaviors.receiveMessagePartial {
      case Protocol.KeyRange(offset, keys) =>
        ctx.log.warn("Page: Addresses - [{}...{}]  Keys - [{}]", prevOffset, offset, keys.mkString(","))
        implicit val s  = ctx.system
        implicit val ec = ctx.executionContext
        val logger      = ctx.log

        if (keys.size >= pageSize) { // for some reason we can get page that's bigger than pageSize
          val f = akka.pattern.after(delay)(Future.successful(logger.warn("Range: [{}]", keys.mkString(","))))
          ctx.pipeToSelf(f) {
            case Success(_)  => Protocol.Written
            case Failure(ex) => Protocol.Fail(ex)
          }
          reading(offset, count + pageSize, delay)
        } else
          keys.size match {
            case 0 =>
              ctx.self.tell(Protocol.Completed(count))
              Behaviors.same
            case 1 =>
              val f = akka.pattern.after(delay)(Future {
                logger.warn("Range: [{}]", keys.mkString(","))
                Protocol.Completed(count + 1)
              })
              ctx.pipeToSelf(f)(mapResult)
              Behaviors.same
            case n =>
              val f = akka.pattern.after(delay)(Future {
                logger.warn("Range: [{}]", keys.mkString(","))
                Protocol.Completed(count + n)
              })
              ctx.pipeToSelf(f)(mapResult)
              Behaviors.same
          }

      case Protocol.Written =>
        ctx.pipeToSelf(readRange(prevOffset, data, pageSize)(ctx.executionContext))(mapResult)
        Behaviors.same

      case Protocol.Completed(num) =>
        ctx.log.warn("★ ★ ★ Recovered {} keys ★ ★ ★", num)
        Behaviors.stopped

      case Protocol.Fail(ex) =>
        ctx.log.error("Failure ", ex)
        Behaviors.stopped
    }
}
