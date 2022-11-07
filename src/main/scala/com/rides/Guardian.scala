package com.rides

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.cluster.Member
import akka.cluster.ddata.durable.raf.RafSerializer
import akka.cluster.ddata.{MMapReader, ReplicatorSettings, SelfUniqueAddress}
import akka.cluster.sharding.external.ExternalShardAllocation
import akka.cluster.sharding.external.scaladsl.ExternalShardAllocationClient
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import akka.cluster.typed.{ClusterSingleton, SelfUp, SingletonActor}
import com.rides.domain.VehicleCmd
import com.rides.state.Vehicle

import scala.collection.immutable
import scala.concurrent.duration.DurationInt

object Guardian {

  private val K = 1024L
  private val M = 1024 * K
  private val G = 1024 * M

  sealed trait Protocol
  object Protocol {
    final case class SelfUpMsg(mba: immutable.SortedSet[Member]) extends Protocol
  }

  def apply(
    dockerHostName: String,
    grpcPort: Int
  ): Behavior[Nothing] =
    Behaviors
      .setup[Protocol] { ctx =>
        implicit val sys               = ctx.system
        implicit val cluster           = akka.cluster.typed.Cluster(sys)
        implicit val selfUniqueAddress = SelfUniqueAddress(cluster.selfMember.uniqueAddress)
        val selfAddress                = selfUniqueAddress.uniqueAddress.address

        ctx.log.warn("★ ★ ★  Step 0. SelfUp: {}  ★ ★ ★", selfUniqueAddress)

        cluster.subscriptions.tell(
          akka.cluster.typed.Subscribe(
            ctx.messageAdapter[SelfUp] { case m: SelfUp =>
              // sort by age, oldest first
              Protocol.SelfUpMsg(immutable.SortedSet.from(m.currentClusterState.members)(Member.ageOrdering))
            },
            classOf[SelfUp]
          )
        )

        Behaviors.receive[Protocol] { case (ctx, _ @Protocol.SelfUpMsg(membersByAge)) =>
          cluster.subscriptions ! akka.cluster.typed.Unsubscribe(ctx.self)

          // Off-heap persistent hash tables.
          /** Memory-mapped(mmap) file I/O is an OS-provided feature that maps the contents of a file on secondary
            * storage into a program’s address space. The program then accesses pages via pointers as if the file
            * resided entirely in memory. The OS transparently loads pages only when the program references them and
            * automatically evicts pages if memory fills up.
            */
          val rafPath = s"./mmap/raf-${selfAddress.host.get + "_" + selfAddress.port.get}"
          val sharedMemoryMap =
            new akka.cluster.ddata.durable.raf.SharedMemoryLongMap(1 << 20, rafPath, 100 * M) // 100 MB
          sharedMemoryMap.setSerializer(new RafSerializer(sys))

          val replicatorCfg = {
            val config = sys.settings.config
            config
              .getConfig("app.replicator.distributed-data")
              .withFallback(config.getConfig("akka.cluster.distributed-data"))
          }
          val setting    = ReplicatorSettings(replicatorCfg)
          val classicCtx = new TypedActorContextOps(ctx).toClassic

          // akka://rides/user/replicator
          // TODO: try ctx.system.systemActorOf()
          val ref =
            classicCtx.actorOf(
              // akka.cluster.ddata.replicator.DDataReplicator2.props(sharedMemoryMap, setting), //VehicleRange

              // TODO:
              // akka.cluster.ddata.replicator.DDataReplicatorRocksDB.props(sharedMemoryMap, setting)
              akka.cluster.ddata.replicator.DDataReplicator.props(sharedMemoryMap, setting),
              "replicator"
            )

          import one.nio.os.*
          ctx.log.warn(
            s"★ ★ ★ 0. PID:{} ${classOf[akka.cluster.ddata.replicator.DDataReplicator].getName}: [{}] : {} ★ ★ ★",
            ProcessHandle.current().pid(),
            ref.path,
            s"""
               | Dispatcher: ${setting.dispatcher}
               | GossipInterval: ${setting.gossipInterval.toSeconds} MaxDeltaElements: ${setting.maxDeltaElements}
               | PruningInterval: ${setting.pruningInterval.toSeconds} PruningMarkerTimeToLive: ${setting.pruningMarkerTimeToLive}
               | Cpus: [${Cpus.PRESENT}:${Cpus.ONLINE}:${Cpus.POSSIBLE}]
               |""".stripMargin
          )

          // Not real usage here, just for logging purposes.
          ctx.spawn(MMapReader(sharedMemoryMap), "mmap-reader")

          /*def newAllocationStrategy() = {
            val leastShardAllocationNew: akka.cluster.sharding.internal.LeastShardAllocationStrategy =
              ShardAllocationStrategy
                .leastShardAllocationStrategy(VehicleRange.NumOfShards / 3, 1)
                .asInstanceOf[akka.cluster.sharding.internal.LeastShardAllocationStrategy]
            //new LeastShardAllocationStrategyWithLogger(leastShardAllocationNew, classicSystem)
          }*/

          val shardRegion: ActorRef[VehicleCmd] =
            ClusterSharding(ctx.system).init(
              Entity(typeKey = Vehicle.TypeKey) { entityCtx =>
                Vehicle(
                  entityCtx.entityId.toLong // PersistenceId.ofUniqueId(entityCtx.entityId)
                )
              // VehicleRange(PersistenceId.ofUniqueId(entityCtx.entityId))
              }
                .withMessageExtractor(Vehicle.shardingMessageExtractor())
                // .withMessageExtractor(VehicleRange.shardingMessageExtractor())
                .withStopMessage(com.rides.domain.StopEntity())
                .withSettings(
                  ClusterShardingSettings(ctx.system)
                    .withPassivationStrategy(
                      akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings.defaults
                        .withIdleEntityPassivation(30.seconds * 6)
                    )
                    // .withStateStoreMode(StateStoreMode.byName(StateStoreModeDData.name))
                    // .withStateStoreMode(StateStoreMode.byName(StateStoreModePersistence.name))
                )
                /*.withAllocationStrategy(
                  new AllocationStrategyLogger(
                    ShardAllocationStrategy.leastShardAllocationStrategy(VehicleRange.numOfShards / 3, 1),
                    sys
                  )
                )*/
                // .withAllocationStrategy(akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy.leastShardAllocationStrategy(Vehicle.numberOfShards / 5, 0.2))
                .withAllocationStrategy(
                  new akka.cluster.sharding.external.ExternalShardAllocationStrategy(sys, Vehicle.TypeKey.name)
                )
            )

          // Should start it on each node
          val client: ExternalShardAllocationClient =
            ExternalShardAllocation(ctx.system).clientFor(Vehicle.TypeKey.name)

          ClusterSingleton(ctx.system)
            .init(
              SingletonActor(
                Behaviors
                  .supervise(ShardRebalancer(client))
                  .onFailure[Exception](SupervisorStrategy.resume.withLoggingEnabled(true)),
                "shard-rebalancer"
              ).withStopMessage(ShardRebalancer.Shutdown)
            )

          import akka.cluster.Implicits.*
          ctx.log.warn("★ ★ ★ 1. {} ShardRegion: {}", Vehicle.TypeKey.name, shardRegion.path)
          ctx.log.warn(
            "★ ★ ★ 2. Singleton(DDataShardCoordinator):{}. Leader:{}",
            membersByAge.head.details,
            cluster.state.leader.getOrElse("none")
          )
          ctx.log.warn("★ ★ ★ 3. All members by age: [{}]", membersByAge.map(_.details).mkString(","))

          import embroidery.*
          ctx.log.info("Location-Tracker".toAsciiArt)

          Bootstrap(
            shardRegion,
            sharedMemoryMap,
            ref,
            dockerHostName,
            grpcPort
          )(ctx.system, cluster)
          Behaviors.same
        }
      }
      .narrow
}
