package com.rides

import akka.actor.RootActorPath
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.cluster.Member
import akka.cluster.ddata.BloomFilterGuardian
import akka.cluster.ddata.ReplicatorSettings
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.ddata.replicator.DDataReplicatorRocksDB
import akka.cluster.ddata.replicator.RocksDbSettings
import akka.cluster.sharding.DynamicLeastShardAllocationStrategy
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.typed.SelfUp
import akka.stream.scaladsl.Source
import com.rides.domain.VehicleCmd
import com.rides.state.Vehicle

import java.io.File
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
        // val selfAddress                = selfUniqueAddress.uniqueAddress.address

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
          /*val rafPath = s"./mmap/raf-${selfAddress.host.get + "_" + selfAddress.port.get}"
          val sharedMemoryMap =
            new akka.cluster.ddata.durable.raf.SharedMemoryLongMap(1 << 20, rafPath, 100 * M) // 100 MB
          sharedMemoryMap.setSerializer(new RafSerializer(sys))*/

          val replicatorCfg = {
            val config = sys.settings.config
            config
              .getConfig("app.replicator.distributed-data")
              .withFallback(config.getConfig("akka.cluster.distributed-data"))
          }
          val setting    = ReplicatorSettings(replicatorCfg)
          val classicCtx = new TypedActorContextOps(ctx).toClassic

          val dbFile = {
            val addr = cluster.selfMember.address
            val seq  = addr.host.flatMap(h => addr.port.map(p => s"$h:$p")).getOrElse(throw new Exception("Boom !!!"))
            new File(s"rocks/rocksdb-$seq")
          }

          val (db, cf) = RocksDbSettings.openRocksDB(dbFile.getAbsolutePath)

          // ctx.actorOf(PropsAdapter(MMapReader(sharedMemoryMap)))
          val (internals, internalSrc) = Source.queue[DDataReplicatorRocksDB.RMsg.Internal](128).preMaterialize()
          val bfActor = ctx.spawn(BloomFilterGuardian(db, cf, internals, setting.maxDeltaElements), "bf")

          val ref =
            classicCtx.actorOf(
              akka.cluster.ddata.replicator.DDataReplicatorRocksDB
                .props(bfActor, internals, internalSrc, db, cf, setting),
              // akka.cluster.ddata.replicator.DDataReplicator.props(sharedMemoryMap, setting), //mmap Vehicle
              // akka.cluster.ddata.replicator.DDataReplicator2.props(sharedMemoryMap, setting), //VehicleRange
              "replicator"
            )

          import one.nio.os.*
          ctx.log.warn(
            s"★ ★ ★ 0. PID:{} ${classOf[akka.cluster.ddata.replicator.DDataReplicator].getName}: [{}] : {} ★ ★ ★",
            ProcessHandle.current().pid(),
            ref.path,
            s"""
               | GossipInterval: ${setting.gossipInterval.toSeconds} MaxDeltaElements: ${setting.maxDeltaElements}
               | PruningInterval: ${setting.pruningInterval.toSeconds} PruningMarkerTimeToLive: ${setting.pruningMarkerTimeToLive}
               | Cpus: [${Cpus.PRESENT}:${Cpus.ONLINE}:${Cpus.POSSIBLE}]
               |""".stripMargin
          )

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
                Vehicle(entityCtx.entityId.toLong) // PersistenceId.ofUniqueId(entityCtx.entityId)
              // Vehicle2(p, entityCtx.entityId.toLong)
              // com.rides.state.VehicleRange(akka.persistence.typed.PersistenceId.ofUniqueId(entityCtx.entityId))
              }
                .withMessageExtractor(Vehicle.shardingMessageExtractor())
                // .withMessageExtractor(VehicleRange.shardingMessageExtractor())
                .withStopMessage(com.rides.domain.StopEntity())
                .withSettings(
                  ClusterShardingSettings(ctx.system)
                    .withPassivationStrategy(
                      // https://doc.akka.io/docs/akka/current/typed/cluster-sharding.html?_ga=2.139914166.482445059.1674934006-268650080.1674934006#active-entity-limits
                      /*akka.cluster.sharding.passivation {
                        strategy = custom-lru-strategy
                        custom-lru-strategy {
                          active-entity-limit = 1000000
                          replacement.policy = least-recently-used
                        }
                      }*/
                      akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings.defaults
                        .withIdleEntityPassivation(60.seconds) // 5 min
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
                // .withAllocationStrategy(new akka.cluster.sharding.external.ExternalShardAllocationStrategy(sys, Vehicle.TypeKey.name))
                .withAllocationStrategy(new DynamicLeastShardAllocationStrategy(1, 10, 2, 0.0))

                /*.withAllocationStrategy(
                  new ShardCoordinator.StartableAllocationStrategy {
                    var rebalancer: ActorRef[ClusterDomainEvent] = _
                    override def start(): Unit =
                      rebalancer = ClusterSingleton(ctx.system)
                        .init(
                          SingletonActor(
                            Behaviors
                              .supervise(ShardRebalancer())
                              .onFailure[Exception](SupervisorStrategy.resume.withLoggingEnabled(true)),
                            "shard-rebalancer"
                          ).withStopMessage(ShardRebalancer.Shutdown)
                        )
                    override def allocateShard(
                      requester: actor.ActorRef,
                      shardId: ShardId,
                      currentShardAllocations: Map[actor.ActorRef, IndexedSeq[ShardId]]
                    ): Future[actor.ActorRef] =
                      Future.successful {
                        println(s"allocateShard $requester $shardId")
                        requester
                      }
                    override def rebalance(
                      currentShardAllocations: Map[actor.ActorRef, IndexedSeq[ShardId]],
                      rebalanceInProgress: Set[ShardId]
                    ): Future[Set[ShardId]] =
                      Future.successful {
                        val cur =
                          currentShardAllocations.map { case (k, v) => s"$k -> [${v.mkString(",")}]" }.mkString(", ")
                        println(s"Rebalance: [$cur] -> [${rebalanceInProgress.mkString(",")}]")
                        Set.empty
                      }
                  }
                )*/
            )

          // Should start it on each node
          /*val client: ExternalShardAllocationClient =
            ExternalShardAllocation(ctx.system).clientFor(Vehicle.TypeKey.name)

          ClusterSingleton(ctx.system)
            .init(
              SingletonActor(
                Behaviors
                  .supervise(
                    ShardRebalancer(
                      client,
                      ctx.system.settings.config.getDuration("akka.cluster.sharding.rebalance-interval").asScala
                    )
                  )
                  .onFailure[Exception](SupervisorStrategy.resume.withLoggingEnabled(true)),
                "shard-rebalancer"
              ).withStopMessage(ShardRebalancer.Shutdown)
            )*/

          import akka.cluster.Implicits.*

          val logger = ctx.log

          logger.warn("★ ★ ★ 1. {} ShardRegion: {}", Vehicle.TypeKey.name, shardRegion.path)
          logger.warn(
            "★ ★ ★ 2. Singleton(DDataShardCoordinator):{}. Leader:{}",
            membersByAge.head.details,
            cluster.state.leader.getOrElse("none")
          )
          logger.warn("★ ★ ★ 3. All members by age: [{}]", membersByAge.map(_.details).mkString(","))

          // Lookup the replicator which is being used by akka.cluster.sharding.DDataShardCoordinator
          val DDataShardReplicatorPath = RootActorPath(ref.path.address) / "system" / "sharding" / "replicator"
          ctx.system.toClassic
            .actorSelection(DDataShardReplicatorPath)
            .resolveOne(3.seconds)
            .foreach { ddataShardReplicator =>
              logger.warn(
                "★ ★ ★ 4. akka.cluster.sharding.distributed-data: {}, {} ",
                ddataShardReplicator.path,
                ctx.system.settings.config.getConfig("akka.cluster.sharding.distributed-data")
              )
              akka.cluster.utils
                .shardingStateChanges2(
                  ddataShardReplicator,
                  ctx.system,
                  cluster.selfMember.address.host.getOrElse("local")
                )
            }(ctx.executionContext)

          import embroidery.*
          logger.info("Location-Tracker".toAsciiArt)

          Bootstrap(
            shardRegion,
            db,
            cf,
            // sharedMemoryMap,
            ref,
            dockerHostName,
            grpcPort
          )(ctx.system, cluster)
          Behaviors.same
        }
      }
      .narrow
}
