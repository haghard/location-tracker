ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.10"

val AkkaVersion = "2.6.20"   //"2.7.0"

val AkkaMngVersion  = "1.1.4"     //"1.2.0"
val AkkaHttpVersion = "10.2.10"   //"10.4.0"
val typesafeConfigVersion = "1.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "location-tracker",

    resolvers ++= Seq(
      Resolver.defaultLocal,
      Resolver.mavenLocal,
      Resolver.mavenCentral,
      Resolver.typesafeRepo("releases"),
      "Hyperreal Repository" at "https://dl.bintray.com/edadma/maven"
    ) ++ Resolver.sonatypeOssRepos("public") ++ Resolver.sonatypeOssRepos("snapshots") ++ Resolver.sonatypeOssRepos("releases")
  )
  .enablePlugins(AkkaGrpcPlugin)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j"  % AkkaVersion,

  "com.typesafe.akka" %% "akka-persistence-typed"       % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed"  % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools"           % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream"                  % AkkaVersion,

  "com.typesafe.akka"             %% "akka-discovery"                     % AkkaVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap"  % AkkaMngVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-http"       % AkkaMngVersion,

  //transport = aeron-udp
  "io.aeron" % "aeron-driver" % "1.40.0",
  "io.aeron" % "aeron-client" % "1.40.0",

  "ch.qos.logback" % "logback-classic" % "1.4.5",
  "org.wvlet.airframe" %% "airframe-ulid" % "23.2.5",

  "ru.odnoklassniki" % "one-nio" % "1.6.1",
  "com.github.wi101" %% "embroidery" % "0.1.1",

  "org.rocksdb" % "rocksdbjni" % "7.9.2", //31 Oct 2022

  //https://alexandrnikitin.github.io/blog/bloom-filter-for-scala/
  //https://hur.st/bloomfilter/
  "com.github.alexandrnikitin" %% "bloom-filter" % "0.13.1",

  "org.bouncycastle" % "bcprov-jdk18on"  % "1.72",

  //https://github.com/sebastian-alfers/akka-grpc-rich-error
  "io.grpc" % "grpc-protobuf" % "1.53.0",

  "com.twitter" %% "util-hashing" % "22.12.0",

  "org.scala-lang" % "scala-reflect" % scalaVersion.value,

  //https://github.com/RoaringBitmap/RoaringBitmap/tree/3f468dc381f20989ac8c7e8b2a3e54bc94f9c64c
  "org.roaringbitmap" % "RoaringBitmap" % "0.9.39",

  "com.lihaoyi" % "ammonite" % "2.5.8" % "test" cross CrossVersion.full
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
  "-language:experimental.macros",
  "-Wnonunit-statement",
  "-target:17",
  "-release:17",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlog-reflective-calls",
  "-Xlint"
)

Compile / javacOptions ++= Seq(
  "-Xlint:unchecked", "-Xlint:deprecation", "-parameters",
)

scalafmtOnCompile := true

run / fork := false


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

ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"

Global / semanticdbEnabled := true
Global / semanticdbVersion := scalafixSemanticdb.revision
Global / watchAntiEntropy := scala.concurrent.duration.FiniteDuration(5, java.util.concurrent.TimeUnit.SECONDS)

//test:run
Test / sourceGenerators += Def.task {
  val file = (Test / sourceManaged).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

addCommandAlias("c", "compile")
addCommandAlias("r", "reload")
addCommandAlias("sfix", "scalafix OrganizeImports; test:scalafix OrganizeImports")
