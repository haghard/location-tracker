package com.rides

import akka.actor.RootActorPath
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.{TypedActorContextOps, TypedActorSystemOps}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.Member
import akka.cluster.ddata.durable.raf.RafSerializer
import akka.cluster.ddata.{MMapReader, ReplicatorSettings, SelfUniqueAddress}
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import akka.cluster.typed.SelfUp
import akka.persistence.typed.PersistenceId
import com.rides.domain.VehicleCmd

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

  def apply(dockerHostName: String, grpcPort: Int): Behavior[Nothing] =
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
          val setting = ReplicatorSettings(replicatorCfg)

          val classicCtx = new TypedActorContextOps(ctx).toClassic

          // akka://rides/user/replicator
          val ref =
            classicCtx.actorOf(
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

          ctx.spawn(MMapReader(sharedMemoryMap), "mmap-reader")

          val shardRegion: ActorRef[VehicleCmd] =
            ClusterSharding(ctx.system).init(
              Entity(typeKey = VehicleStateBased.TypeKey) { entityCtx =>
                VehicleStateBased(PersistenceId.ofUniqueId(entityCtx.entityId))
              }
                .withMessageExtractor(VehicleStateBased.shardingMessageExtractor(selfUniqueAddress))
                .withStopMessage(com.rides.domain.StopEntity())
                .withSettings(
                  ClusterShardingSettings(ctx.system)
                    .withPassivationStrategy(
                      akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings.defaults
                        .withIdleEntityPassivation(30.seconds)
                    )
                    // .withStateStoreMode(StateStoreMode.byName(StateStoreModeDData.name))
                    // .withStateStoreMode(StateStoreMode.byName(StateStoreModePersistence.name))
                )
                /*.withAllocationStrategy(
                  akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
                    .leastShardAllocationStrategy(VehicleStateBased.numberOfShards / 5, 0.2)
                )*/
                // try this
                // https://doc.akka.io/docs/akka/current/typed/cluster-sharding.html?_ga=2.225099034.1493026331.1653164470-1899964194.1652800389#external-shard-allocation
                // .withAllocationStrategy(new ExternalShardAllocationStrategy(sys, OrderManagement.TypeKey.name))
            )

          /** Looks up the replicator that's being used by [[akka.cluster.sharding.DDataShardCoordinator]]
            */
          val DDataShardReplicatorPath =
            RootActorPath(sys.deadLetters.path.address) / "system" / "sharding" / "replicator"
          sys.toClassic
            .actorSelection(DDataShardReplicatorPath)
            .resolveOne(5.seconds)
            .foreach { ddataShardReplicator =>
              akka.cluster.utils
                .shardingStateChanges(ddataShardReplicator, sys, cluster.selfMember.address.host.getOrElse("local"))
            }(sys.executionContext)

          import akka.cluster.Implicits.*
          ctx.log.warn("★ ★ ★ 1. {} ShardRegion: {}", VehicleStateBased.TypeKey.name, shardRegion.path)
          ctx.log.warn(
            "★ ★ ★ 2. Singleton_oldest(DDataShardCoordinator):{}. Leader:{}",
            membersByAge.head.details,
            cluster.state.leader.getOrElse("none")
          )
          ctx.log.warn("★ ★ ★ 3. All members by age: [{}]", membersByAge.map(_.details).mkString(","))

          import embroidery._
          ctx.log.info("Location tracker".toAsciiArt)

          Bootstrap(shardRegion, sharedMemoryMap, ref, dockerHostName, grpcPort)(ctx.system, cluster)
          Behaviors.same
        }
      }
      .narrow
}
