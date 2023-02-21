package com.rides

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.CoordinatedShutdown.PhaseActorSystemTerminate
import akka.actor.CoordinatedShutdown.PhaseBeforeServiceUnbind
import akka.actor.CoordinatedShutdown.PhaseServiceRequestsDone
import akka.actor.CoordinatedShutdown.PhaseServiceStop
import akka.actor.CoordinatedShutdown.PhaseServiceUnbind
import akka.actor.CoordinatedShutdown.Reason
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.cluster.typed.Cluster
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import com.rides.domain.VehicleCmd
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.RocksDB

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

object Bootstrap {

  private final case object BindFailure extends Reason
}

final case class Bootstrap(
  shardRegion: ActorRef[VehicleCmd],
  // sharedMemoryMap: akka.cluster.ddata.durable.raf.SharedMemoryLongMap,
  db: RocksDB,
  columnFamily: ColumnFamilyHandle,
  ddataReplicator: akka.actor.ActorRef,
  bindHost: String,
  port: Int
)(implicit
  system: ActorSystem[_],
  cluster: Cluster
) {

  implicit val ex = system.executionContext
  val config      = system.settings.config

  val terminationDeadline =
    config.getDuration("akka.coordinated-shutdown.default-phase-timeout").asScala

  val shutdown = CoordinatedShutdown(system)

  /** https://en.wikipedia.org/wiki/Little%27s_law
    *
    * L = λ * W L – the average number of items in a queuing system (queue size) λ – the average number of items
    * arriving at the system per unit of time W – the average waiting time an item spends in a queuing system
    *
    * Question: What parallelism factor we need given throughput = 120rps and average latency = 0.05 (50millis) ?
    *
    * 120 * 0.05 = 6
    */
  val grpcService: HttpRequest => Future[HttpResponse] =
    VehicleServiceHandler.withServerReflection(
      new VehicleServiceApi(
        db,
        columnFamily,
        // sharedMemoryMap,
        // ddataReplicator,
        cluster,
        VehicleApi(
          shardRegion,
          ddataReplicator,
          "api",
          bufferSize = 120,
          parallelism = 6,
          akka.util.Timeout.create(config.getDuration("location-tracker.ask-timeout"))
        )
      )
    )

  Http(system)
    .newServerAt(bindHost, port)
    // .newServerAt("0.0.0.0", 8080)
    .bind(grpcService)
    .onComplete {
      case Failure(ex) =>
        system.log.error(s"Shutting down because can't bind to $bindHost:$port", ex)
        shutdown.run(Bootstrap.BindFailure)
      case Success(binding) =>
        system.log.info("★ ★ ★ ★ ★ ★ ★ ★ ★ ActorSystem({}) tree ★ ★ ★ ★ ★ ★ ★ ★ ★", system.name)
        system.log.info(system.printTree)
        // binding.addToCoordinatedShutdown(terminationDeadline)

        shutdown.addTask(PhaseBeforeServiceUnbind, "before-unbind") { () =>
          Future.successful {
            system.log.info("★ ★ ★ CoordinatedShutdown [before-unbind] ★ ★ ★")
            Done
          }
        }

        // Next 2 tasks(PhaseServiceUnbind, PhaseServiceRequestsDone) makes sure that during shutdown
        // no more requests are accepted and
        // all in-flight requests have been processed

        shutdown.addTask(PhaseServiceUnbind, "http-unbind") { () =>
          // No new connections are accepted. Existing connections are still allowed to perform request/response cycles
          binding.unbind().map { done =>
            system.log.info("★ ★ ★ CoordinatedShutdown [http-api.unbind] ★ ★ ★")
            done
          }
        }

        // graceful termination request being handled on this connection
        shutdown.addTask(PhaseServiceRequestsDone, "http-terminate") { () =>
          /** It doesn't accept new connection but it drains the existing connections Until the `terminationDeadline`
            * all the req that have been accepted will be completed and only than the shutdown will continue
            */

          binding.terminate(terminationDeadline).map { _ =>
            system.log.info("★ ★ ★ CoordinatedShutdown [http-api.terminate]  ★ ★ ★")
            Done
          }
        }

        // forcefully kills connections that are still open
        shutdown.addTask(PhaseServiceStop, "close.connections") { () =>
          Http().shutdownAllConnectionPools().map { _ =>
            system.log.info("★ ★ ★ CoordinatedShutdown [close.connections] ★ ★ ★")
            Done
          }
        }

        shutdown.addTask(PhaseActorSystemTerminate, "system.term") { () =>
          Future.successful {
            system.log.info("★ ★ ★ CoordinatedShutdown [close.connections] ★ ★ ★")
            Done
          }
        }
    }
}
