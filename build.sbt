ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.8"
val AkkaVersion = "2.6.20"
val AkkaMngVersion  = "1.1.4"
val AkkaHttpVersion = "10.2.10"
val typesafeConfigVersion = "1.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "location-tracker"
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
  "io.aeron" % "aeron-driver" % "1.39.0",
  "io.aeron" % "aeron-client" % "1.39.0",

  "ch.qos.logback" % "logback-classic" % "1.4.0",
  "org.wvlet.airframe" %% "airframe-ulid" % "22.7.3",
  "ru.odnoklassniki" % "one-nio" % "1.5.0",
  "com.github.wi101" %% "embroidery" % "0.1.1",

  "com.lihaoyi" % "ammonite" % "2.5.4" % "test" cross CrossVersion.full
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
  "-target:14",
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

dependencyOverrides ++= Seq(
  "com.typesafe"      %  "config"                       % typesafeConfigVersion,
  "com.typesafe.akka" %% "akka-actor-typed"             % AkkaVersion,
  "com.typesafe.akka" %% "akka-protobuf"                % AkkaVersion,
  "com.typesafe.akka" %% "akka-protobuf-v3"             % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence"             % AkkaVersion,
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
