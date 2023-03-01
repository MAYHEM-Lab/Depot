import scala.sys.process._

lazy val stage = taskKey[Unit]("Stage package assets")
lazy val docker = taskKey[Unit]("Build and package Docker images")
lazy val publish = taskKey[Unit]("Publish Docker images")
val DockerSettings = Seq(
  stage := None,
  docker := s"docker build -t racelab/depot-${name.value} . -f ${baseDirectory.value / "Dockerfile"}".!,
  docker := docker.dependsOn(stage).value,
  publish := s"docker push racelab/depot-${name.value}".!,
  publish := publish.dependsOn(docker).value
)

lazy val client = (project in file("client"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src"
  )

lazy val kernel = (project in file("kernel"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    autoScalaLibrary := false,
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src"
  )
  .dependsOn(client)

lazy val spark = (project in file("cluster/spark"))
  .disablePlugins(AssemblyPlugin)
  .settings(DockerSettings)

lazy val executor = (project in file("cluster/executor"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    DockerSettings,
    docker := docker.dependsOn(spark / docker).value,
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src"
  )
  .dependsOn(spark)

lazy val router = (project in file("cluster/router"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    DockerSettings,
    docker := docker.dependsOn(spark / docker).value,
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src"
  )

lazy val transformer = (project in file("cluster/transformer"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    DockerSettings,
    docker := docker.dependsOn(spark / docker).value,
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src"
  )

lazy val frontend = (project in file("frontend"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    DockerSettings,
    target := baseDirectory.value / "build",
    stage := (Process("yarn install", baseDirectory.value) #&& Process("yarn build", baseDirectory.value)).!,
    clean := IO.delete(target.value),
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src"
  )

lazy val manager = (project in file("manager"))
  .enablePlugins(AssemblyPlugin)
  .settings(
    DockerSettings,
    organization := "wtf.knc.depot",
    scalaVersion := "2.13.8",
    resolvers += "Akka Repository" at "https://repo.akka.io/releases/",
    stage := assembly.value,
    run / fork := true,
    libraryDependencies ++= {
      val logbackVersion = "1.2.10"
      val mysqlConnectorVersion = "8.0.28"
      val finagleVersion = "22.1.0"
      val flywayVersion = "8.5.1"
      val rabbitmqVersion = "5.14.2"
      val jwtsVersion = "9.0.5"
      val jets3tVersion = "0.9.7"
      Seq(
        "com.twitter" %% "inject-request-scope" % finagleVersion,
        "com.twitter" %% "inject-app" % finagleVersion,
        "com.twitter" %% "util-core" % finagleVersion,
        "com.twitter" %% "util-jackson" % finagleVersion,
        "com.twitter" %% "finagle-http" % finagleVersion,
        "com.twitter" %% "finagle-mysql" % finagleVersion,
        "com.twitter" %% "finatra-http-server" % finagleVersion,
        "com.twitter" %% "finatra-http-client" % finagleVersion,
        "com.twitter" %% "twitter-server-logback-classic" % finagleVersion,
        "ch.qos.logback" % "logback-classic" % logbackVersion,
        "mysql" % "mysql-connector-java" % mysqlConnectorVersion,
        "com.rabbitmq" % "amqp-client" % rabbitmqVersion,
        "com.github.jwt-scala" %% "jwt-core" % jwtsVersion,
        "org.flywaydb" % "flyway-core" % flywayVersion,
        "org.flywaydb" % "flyway-mysql" % flywayVersion,
        "org.jets3t" % "jets3t" % jets3tVersion,
        "org.apache.beam" % "beam-sdks-java-io-kafka" % "2.17.0",
        "org.apache.beam" % "beam-sdks-java-core" % "2.40.0",
        "org.apache.beam" % "beam-model-pipeline" % "2.40.0",
        "org.apache.beam" % "beam-runners-direct-java" % "2.40.0",
        "com.google.api-client" % "google-api-client" % "1.31.1",
        "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % "2.40.0",
        "org.apache.kafka" %% "kafka" % "3.4.0"
      )
    },
    excludeDependencies ++= Seq(
      ExclusionRule("log4j", "log4j"),
      ExclusionRule("org.slf4j", "slf4j-log4j12"),
      ExclusionRule("commons-logging", "commons-logging")
    ),
    assemblyJarName := "server.jar",
    assemblyMergeStrategy := {
      case PathList(ps @ _*) if Seq(".class", ".xsd", ".properties", ".dtd").exists(ps.last.endsWith) =>
        MergeStrategy.first
      case "BUILD" => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
      case other => MergeStrategy.defaultMergeStrategy(other)
    }
  )
