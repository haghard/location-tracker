package akka.cluster

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import com.rides.domain.VehicleCmd

import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/** Buffers up incoming messages for flushPeriod before sending them to the local shard region.
  */
object ShardRegionDrainingBuffer {

  trait Ack
  object Ack extends Ack

  type Element = (akka.actor.ActorRef, VehicleCmd)

  sealed trait Cmd

  final case class Next(ackTo: ActorRef[Ack], e: Element) extends Cmd
  final case class Connect(ackTo: ActorRef[Ack])          extends Cmd
  case object Complete                                    extends Cmd
  final case class Fail(ex: Throwable)                    extends Cmd
  case object Flush                                       extends Cmd

  private case object Stop extends Cmd

  /** @param drainPeriod
    *   - Timeout that should be big enough to process the buffer completely.
    * @param completion
    *   - Promise to signal completion.
    */
  def apply(
    drainPeriod: FiniteDuration,
    completion: Promise[akka.Done],
    flushPeriod: FiniteDuration = 250.millis
  ): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      Behaviors.receiveMessage {
        case Connect(ackTo) =>
          ackTo.tell(Ack)
          Behaviors.withTimers { timer =>
            timer.startTimerWithFixedDelay(Flush, flushPeriod)
            // new MessageBufferMap[ShardId]
            active(akka.util.MessageBuffer.empty, drainPeriod, completion)(ctx)
          }
        case other =>
          ctx.log.error(s"Unexpected $other in ShardRegionDrainingBuffer.apply")
          Behaviors.stopped
      }
    }

  def flush(
    buffer: akka.util.MessageBuffer,
    drainPeriod: FiniteDuration,
    completion: Promise[akka.Done]
  )(implicit ctx: ActorContext[Cmd]): Behavior[Cmd] = {
    buffer.foreach { case (m, ref) => ref ! m }
    ctx.log.info("Flushing rest of the buffer: {}", buffer.size)
    ctx.scheduleOnce(drainPeriod, ctx.self, Stop)
    stopping(completion)
  }

  def active(
    buffer: akka.util.MessageBuffer,
    drainPeriod: FiniteDuration,
    completion: Promise[akka.Done]
  )(implicit ctx: ActorContext[Cmd]): Behavior[Cmd] =
    Behaviors
      .receiveMessagePartial[Cmd] {
        case n: Next =>
          buffer.append(n.e._2, n.e._1)
          n.ackTo.tell(Ack)
          Behaviors.same

        case Flush =>
          if (buffer.nonEmpty) {
            // ctx.log.info(s"Flush {}", buffer.size)
            buffer.foreach { case (cmd, ref) =>
              ref ! cmd
              buffer.dropHead()
            }
          }
          Behaviors.same

        case Complete =>
          ctx.log.info("Queue completed")
          flush(buffer, drainPeriod, completion)

        case Fail(ex) =>
          ctx.log.warn(s"Queue completed with ${ex.getClass.getName}")
          flush(buffer, drainPeriod, completion)
      }

  private def stopping(
    completion: Promise[akka.Done]
  ): Behavior[Cmd] =
    Behaviors.receiveMessagePartial {
      case Flush =>
        Behaviors.same
      case Stop =>
        completion.success(akka.Done)
        Behaviors.stopped
    }
}
