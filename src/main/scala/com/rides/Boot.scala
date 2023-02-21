package com.rides

import akka.actor.typed.ActorSystem
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.io.File

object Boot extends Ops {

  val AkkaSystemName = "rides"

  val HTTP_PORT_VAR      = "GRPC_PORT"
  val CONTACT_POINTS_VAR = "CONTACT_POINTS"

  def main(args: Array[String]): Unit = {

    val opts: Map[String, String] = argsToOpts(args.toList)
    applySystemProperties(opts)

    val configFile = new File("./src/main/resources/application.conf")

    val akkaPort = sys.props
      .get("akka.remote.artery.canonical.port")
      .flatMap(_.toIntOption)
      .getOrElse(throw new Exception("akka.remote.artery.canonical.port not found"))

    val grpcPort = sys.props
      .get(HTTP_PORT_VAR)
      .flatMap(_.toIntOption)
      .getOrElse(throw new Exception(s"$HTTP_PORT_VAR not found"))

    val contactPoints =
      sys.props
        .get(CONTACT_POINTS_VAR)
        .map(_.split(","))
        .getOrElse(throw new Exception(s"$CONTACT_POINTS_VAR not found"))

    if (contactPoints.size != 2)
      throw new Exception(s"$CONTACT_POINTS_VAR expected size should be 2")

    val hostName = sys.props
      .get("akka.remote.artery.canonical.hostname")
      .getOrElse(throw new Exception("akka.remote.artery.canonical.hostname is expected"))

    val dockerHostName = internalDockerAddr
      .map(_.getHostAddress)
      // .getOrElse("0.0.0.0")
      .getOrElse(hostName) // TODO: for local debug only !!!!!!!!!!!!!!!!!

    val managementPort = grpcPort - 1

    applySystemProperties(
      Map(
        "-Dakka.management.http.hostname"      -> hostName,
        "-Dakka.management.http.port"          -> managementPort.toString,
        "-Dakka.remote.artery.bind.hostname"   -> dockerHostName,
        "-Dakka.remote.artery.bind.port"       -> akkaPort.toString,
        "-Dakka.management.http.bind-hostname" -> dockerHostName,
        "-Dakka.management.http.bind-port"     -> managementPort.toString
      )
    )

    val config: Config = {
      val bootstrapEndpoints = {
        val endpointList = contactPoints.map(s => s"{host=$s,port=$managementPort}").mkString(",")
        ConfigFactory
          .parseString(s"akka.discovery.config.services { $AkkaSystemName = { endpoints = [ $endpointList ] }}")
          .resolve()
      }
      bootstrapEndpoints
        .withFallback(ConfigFactory.parseFile(configFile).resolve())
        .withFallback(ConfigFactory.load())
    }

    val system = ActorSystem[Nothing](Guardian(dockerHostName, grpcPort), AkkaSystemName, config)
    akka.management.scaladsl.AkkaManagement(system).start()
    akka.management.cluster.bootstrap.ClusterBootstrap(system).start()
    akka.discovery.Discovery(system).loadServiceDiscovery("config") // kubernetes-api

    // TODO: for local debug only !!!!!!!!!!!!!!!!!!!
    val _ = scala.io.StdIn.readLine()
    system.log.warn("★ ★ ★ ★ ★ ★  Shutting down ... ★ ★ ★ ★ ★ ★")
    system.terminate()
    scala.concurrent.Await.result(
      system.whenTerminated,
      scala.concurrent.duration
        .DurationLong(
          config
            .getDuration("akka.coordinated-shutdown.default-phase-timeout", java.util.concurrent.TimeUnit.SECONDS)
        )
        .seconds
    )
  }
}

/*

class Boot(system: ActorSystem[SpawnProtocol.Command]) {

  implicit val sys = system
  implicit val ex = system.executionContext

  implicit val sch = system.scheduler
  implicit val to = akka.util.Timeout(2.seconds)

  def run(): Future[Http.ServerBinding] =
    system
      .ask { ref: ActorRef[ActorRef[OrderCmd]] =>
        SpawnProtocol.Spawn(Guardian(), "guardian", Props.empty, ref)
      }
      .flatMap { guardian =>

        val grpcService: HttpRequest => Future[HttpResponse] =
          OrderServiceHandler.withServerReflection(
            new OrderServiceApi(
              OrderApi(
                // guardian,
                "api",
                1 << 5,
                8,
                akka.util.Timeout.create(sys.settings.config.getDuration("orders-service.ask-timeout")))))

        val bound = Http(system)
          .newServerAt("0.0.0.0", 8080)
          .bind(grpcService)
          .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 3.seconds))

        bound.onComplete {
          case Success(binding) =>
            val address = binding.localAddress
            system.log.info("★ ★ ★ ★ ★ ★ ★ ★ ★ ActorSystem({}) tree ★ ★ ★ ★ ★ ★ ★ ★ ★", system.name)
            system.log.info(system.printTree)
            /*
             ⌊-> system LocalActorRef class akka.actor.LocalActorRefProvider$SystemGuardian status=0 7 children
             |   ⌊-> IO-TCP RepointableActorRef class akka.io.TcpManager status=0 1 children
             |   |   ⌊-> selectors RoutedActorRef class akka.routing.RouterPoolActor status=0 1 children
             |   |       ⌊-> $a LocalActorRef class akka.io.SelectionHandler status=0 1 children
             |   |           ⌊-> 0 LocalActorRef class akka.io.TcpListener status=0 no children
             |   ⌊-> Materializers RepointableActorRef class akka.stream.impl.MaterializerGuardian status=0 1 children
             |   |   ⌊-> StreamSupervisor-0 LocalActorRef class akka.stream.impl.StreamSupervisor status=0 2 children
             |   |       ⌊-> flow-0-0-ignoreSink RepointableActorRef class akka.stream.impl.fusing.ActorGraphInterpreter status=0 no children
             |   |       ⌊-> flow-1-0-ignoreSink RepointableActorRef class akka.stream.impl.fusing.ActorGraphInterpreter status=0 no children
             |   ⌊-> deadLetterListener RepointableActorRef class akka.event.DeadLetterListener status=0 no children
             |   ⌊-> eventStreamUnsubscriber-1 RepointableActorRef class akka.event.EventStreamUnsubscriber status=0 no children
             |   ⌊-> localReceptionist RepointableActorRef class akka.actor.typed.internal.adapter.ActorAdapter status=0 no children
             |   ⌊-> log1-Slf4jLogger RepointableActorRef class akka.event.slf4j.Slf4jLogger status=0 no children
             |   ⌊-> pool-master RepointableActorRef class akka.http.impl.engine.client.PoolMasterActor status=0 no children
             ⌊-> user LocalActorRef class akka.actor.typed.internal.adapter.ActorAdapter status=0 1 children
             ⌊-> guardian LocalActorRef class akka.actor.typed.internal.adapter.ActorAdapter status=0 no children
 */
            system.log.info("★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★")
            system.log.info("gRPC server bound to {}:{}", address.getHostString, address.getPort)
          case Failure(ex) =>
            system.log.error("Failed to bind gRPC endpoint, terminating system", ex)
            system.terminate()
        }
        bound
      }
}
 */
