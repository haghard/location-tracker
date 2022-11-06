ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.10"
val AkkaVersion = "2.6.20"        //"2.7.0"
val AkkaMngVersion  = "1.1.4"     //"1.2.0"
val AkkaHttpVersion = "10.2.10"   //"10.4.0"
val typesafeConfigVersion = "1.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "location-tracker"
  )
  .enablePlugins(AkkaGrpcPlugin)

val AkkaPersistenceR2dbcVersion = "1.0.0"
val AkkaProjectionVersion = "1.3.0" //"1.2.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j"  % AkkaVersion,

  "com.typesafe.akka" %% "akka-persistence-typed"       % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed"  % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools"           % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream"                  % AkkaVersion,

  "com.typesafe.akka"             %% "akka-discovery"                     % AkkaVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap"  % AkkaMngVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-http"       % AkkaMngVersion,

  "org.scala-lang.modules"        %% "scala-collection-compat"            % "2.8.1",

  //transport = aeron-udp
  "io.aeron" % "aeron-driver" % "1.40.0",
  "io.aeron" % "aeron-client" % "1.40.0",

  "ch.qos.logback" % "logback-classic" % "1.4.4",
  "org.wvlet.airframe" %% "airframe-ulid" % "22.10.4",

  "ru.odnoklassniki" % "one-nio" % "1.5.0",
  "com.github.wi101" %% "embroidery" % "0.1.1",

  "org.hdrhistogram"  %   "HdrHistogram"         %  "2.1.12",

  "org.rocksdb" % "rocksdbjni" % "7.6.0", //31 Oct 2022

  //https://github.com/akka/akka-projection/releases
  //https://github.com/akka/akka-projection/blob/main/samples/grpc/shopping-cart-service-scala/build.sbt
  //https://www.lightbend.com/blog/ditch-the-message-broker-go-faster


  // 4. Querying and publishing data from Akka Persistence
  "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion,

  //"com.lightbend.akka" %% "akka-projection-r2dbc" % AkkaPersistenceR2dbcVersion,
  "com.lightbend.akka" %% "akka-projection-grpc" % AkkaProjectionVersion,
  "com.lightbend.akka" %% "akka-projection-durable-state" % AkkaProjectionVersion,

  //"com.lightbend.akka" %% "akka-projection-eventsourced" % AkkaProjectionVersion,
  //"com.lightbend.akka" %% "akka-projection-testkit" % AkkaProjectionVersion % Test)


  //"io.rsocket" %% "rsocket" % "core"  % "1.0.0"
  //https :// github.com / rsocket / rsocket - java

  //"com.lihaoyi" % "ammonite" % "2.5.5" % "test" cross CrossVersion.full
  "com.lihaoyi" % "ammonite" % "2.5.5-15-277624cf" % "test" cross CrossVersion.full
)

addCommandAlias("c", "compile")
addCommandAlias("r", "reload")

enablePlugins(JavaAppPackaging, DockerPlugin)
dockerBaseImage := "docker.io/library/adoptopenjdk:14-jre-hotspot"
dockerUsername := sys.props.get("docker.username")
dockerRepository := sys.props.get("docker.registry")
dockerUpdateLatest := true
ThisBuild / dynverSeparator := "-"

Compile / scalacOptions ++= Seq(
  "-Xsource:3",
  "-Wnonunit-statement",
  //"-target:14",
  "-release:14",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlog-reflective-calls",
  "-Xlint"
)

Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation", "-parameters") // for Jackson


/*
val consoleDisabledOptions = Seq("-Xfatal-warnings", "-Ywarn-unused", "-Ywarn-unused-import")
scalacOptions in (Compile, console) ~= (_ filterNot consoleDisabledOptions.contains)
*/

scalafmtOnCompile := true

//run / fork := true

run / fork := false
//Global / cancelable := false // ctrl-c

// akka-discovery, akka-distributed-data
dependencyOverrides ++= Seq(
  "com.typesafe"      %  "config"                       % typesafeConfigVersion,
  "com.typesafe.akka" %% "akka-actor-typed"             % AkkaVersion,
  "com.typesafe.akka" %% "akka-protobuf"                % AkkaVersion,
  "com.typesafe.akka" %% "akka-protobuf-v3"             % AkkaVersion,

  "com.typesafe.akka" %% "akka-cluster-sharding"        % AkkaVersion,
  "com.typesafe.akka" %% "akka-discovery"               % AkkaVersion,
  "com.typesafe.akka" %% "akka-distributed-data"        % AkkaVersion,

  "com.typesafe.akka" %% "akka-persistence"             % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query"       % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed"       % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor"                   % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster"                 % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed"  % AkkaVersion,
  "com.typesafe.akka" %% "akka-coordination"            % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream"                  % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools"           % AkkaVersion,
  "com.typesafe.akka" %% "akka-http"                    % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-core"               % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json"         % AkkaHttpVersion,
)

//test:run
Test / sourceGenerators += Def.task {
  val file = (Test / sourceManaged).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue
