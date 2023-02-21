package akka.cluster

import akka.Done
import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.stream.*
import akka.stream.scaladsl.Source
import akka.stream.typed.scaladsl.ActorSink

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object ApiProcessor {

  /** Signals that a request cannot be handled at this time.
    *
    * @param name
    *   name of the processor
    */
  final case class ProcessorOverloaded(name: String)
      extends Exception(s"Processor $name cannot accept requests at this time!")

  /** Signals an unexpected result of calling [[ApiProcessor.offer]].
    *
    * @param cause
    *   the underlying erroneous `QueueOfferResult`, e.g. `Failure` or `QueueClosed`
    */
  /*final case class ProcessorError(cause: QueueOfferResult)
    extends Exception(s"QueueOfferResult $cause was not expected!")*/

  def apply(bufferDrainPeriod: FiniteDuration, bufferSize: Int)(implicit sys: ActorSystem): ApiProcessor =
    new ApiProcessor(bufferDrainPeriod, bufferSize)
}

// format: off
/** Why ApiProcessor ?
 *
 * Does ClusterSharding delay CoordinatedShutdown if it sees that there are unprocessed messages in its internal buffer ?
 * The answer is No.
 *
 * How can we guarantee that all accepted requests can run till completion and all replies are sent back?
 *
 * Flow steps:
 *   0. Stop accepting requests (ClusterSharding cannot do that).
 *   1. Delays CoordinatedShutdown until all accepted requests will be drained, and only then CoordinatedShutdown continues.
 *   2. Back-pressure for both http and remote sharding channels.
 */
// format: on
final class ApiProcessor private (bufferDrainPeriod: FiniteDuration, bufferSize: Int)(implicit
  sys: ActorSystem
) {

  val name = "api-processor"

  require(bufferDrainPeriod > Duration.Zero, s"timeout for processor $name must be > 0, but was $bufferDrainPeriod!")
  require(bufferSize > 0, s"bufferSize for processor $name must be > 0, but was $bufferSize!")

  private implicit val ec = sys.dispatchers.internalDispatcher

  private val (queue, drainDone) = {
    val completion = Promise[akka.Done]()

    // This queue acts as a replacement for
    // `akka.cluster.sharding.buffer-size` (Maximum number of messages that are buffered by a ShardRegion actor)
    val (q, srcQueue) = Source.queue[ShardRegionDrainingBuffer.Element](bufferSize).preMaterialize()

    val shardRegionSink =
      ActorSink
        .actorRefWithBackpressure[
          ShardRegionDrainingBuffer.Element,
          ShardRegionDrainingBuffer.Cmd,
          ShardRegionDrainingBuffer.Ack
        ](
          sys.spawn(ShardRegionDrainingBuffer(bufferDrainPeriod, completion), "shard-sink"),
          ShardRegionDrainingBuffer.Next(_, _),
          ShardRegionDrainingBuffer.Connect(_),
          ShardRegionDrainingBuffer.Ack,
          onCompleteMessage = ShardRegionDrainingBuffer.Complete,
          onFailureMessage = ShardRegionDrainingBuffer.Fail(_)
        )
        .withAttributes(Attributes.inputBuffer(bufferSize, bufferSize))

    srcQueue
      .to(shardRegionSink)
      .addAttributes(ActorAttributes.supervisionStrategy(resume))
      .run()

    (q, completion.future)
  }

  CoordinatedShutdown(sys)
    .addTask(CoordinatedShutdown.PhaseServiceUnbind, "prestop") { () =>
      Future.successful {
        sys.log.info(s"★ ★ ★ CoordinatedShutdown.1 [pre-stop.{}]  ★ ★ ★", queue.size())
        akka.Done
      }
    }

  // CoordinatedShutdown.PhaseServiceStop
  // PhaseServiceUnbind
  CoordinatedShutdown(sys)
    .addTask(CoordinatedShutdown.PhaseServiceUnbind, s"drain.$name") { () =>
      // 2. Close the queue
      Future {
        sys.log.info(s"★ ★ ★ CoordinatedShutdown.2 [drain.queue.{}]  ★ ★ ★", queue.size())
        complete()
        akka.Done
      }
    }

  // Why this phase? Because it's the closest to PhaseClusterShardingShutdownRegion
  CoordinatedShutdown(sys).addTask(CoordinatedShutdown.PhaseBeforeClusterShutdown, s"delay.drain.$name")(() =>
    whenDone(System.currentTimeMillis())
  )

  /** Offer the given request to the process. The returned `Future` is either completes successfully with the response
    * or fails with [[ApiProcessor.ProcessorOverloaded]], if the process back-pressures.
    *
    * @param element
    *   request to be offered
    * @return
    *   eventual response
    */
  def offer(element: ShardRegionDrainingBuffer.Element): QueueOfferResult =
    queue.offer(element)

  /** Shutdown this processor. Already accepted requests are completed, but no new ones are accepted. To watch shutdown
    * completion use [[whenDone]].
    */
  def complete(): Unit =
    // if (logger.isWarnEnabled) logger.warn(s"Shutdown for $name requested with qSize:${qSize} !")
    queue.complete()

  /** The returned `Future` is completed when the running process is completed, e.g. via [[complete]] or unexpected
    * failure.
    *
    * @return
    *   completion signal
    */
  def whenDone(start: Long): Future[Done] =
    drainDone
      .recover { case NonFatal(_) => akka.Done }
      .flatMap { _ =>
        akka.pattern.after(100.millis) {
          Future {
            val delay = System.currentTimeMillis() - start
            sys.log.info(s"★ ★ ★ CoordinatedShutdown.2 [delay.queue.drained] continue after {} ★ ★ ★", delay)
            akka.Done
          }
        }
      }

  private def resume(cause: Throwable) =
    // if (logger.isErrorEnabled) logger.error(s"$name failed and resumes", cause)
    Supervision.Resume
}
