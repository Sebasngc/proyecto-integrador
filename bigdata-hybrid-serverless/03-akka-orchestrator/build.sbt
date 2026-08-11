// =============================================================================
//  Orquestador basado en el modelo de actores.
//
//  IMPORTANTE — licencia: Akka 2.7+ usa la Business Source License (BSL 1.1),
//  gratuita sólo por debajo de 25 M USD de facturación anual. Para un proyecto
//  académico no hay problema, pero si esto fuera a producción comercial la
//  alternativa es Apache Pekko (fork Apache 2.0). Migración: sustituir el
//  bloque de dependencias por el comentado más abajo y `akka.` -> `org.apache.pekko.`
//  en los imports.
// =============================================================================

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.bigdata"
ThisBuild / version      := "1.0.0"

val AkkaVersion     = "2.6.20"
val AkkaHttpVersion  = "10.2.10"
val AwsSdkVersion   = "2.25.60"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name := "bigdata-orchestrator",

    libraryDependencies ++= Seq(
      // --- núcleo de actores ---
      "com.typesafe.akka" %% "akka-actor-typed"           % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream"                % AkkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"                 % AkkaVersion,

      // --- persistencia (event sourcing del orquestador) ---
      "com.typesafe.akka" %% "akka-persistence-typed"     % AkkaVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,

      // --- HTTP entrante y saliente ---
      "com.typesafe.akka" %% "akka-http"                  % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json"       % AkkaHttpVersion,

      // --- persistencia del resultado ---
      "software.amazon.awssdk" % "dynamodb"               % AwsSdkVersion,

      // --- logging ---
      "ch.qos.logback"    %  "logback-classic"            % "1.5.6",

      // --- tests ---
      "com.typesafe.akka" %% "akka-actor-testkit-typed"   % AkkaVersion     % Test,
      "com.typesafe.akka" %% "akka-http-testkit"          % AkkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-persistence-testkit"   % AkkaVersion     % Test,
      "org.scalatest"     %% "scalatest"                  % "3.2.18"        % Test
    ),

    // ---- alternativa Apache Pekko (Apache 2.0) ----
    // libraryDependencies ++= Seq(
    //   "org.apache.pekko" %% "pekko-actor-typed"       % "1.0.2",
    //   "org.apache.pekko" %% "pekko-persistence-typed" % "1.0.2",
    //   "org.apache.pekko" %% "pekko-http"              % "1.0.1",
    //   "org.apache.pekko" %% "pekko-http-spray-json"   % "1.0.1"
    // ),

    scalacOptions ++= Seq(
      "-deprecation", "-feature", "-unchecked",
      "-Xlint", "-Ywarn-dead-code",
      // Avisa si un match sobre un ADT sellado no es exhaustivo: en un sistema de
      // actores, un caso sin tratar es un mensaje perdido en silencio.
      // NO se usa -Xfatal-warnings: cualquier warning menor (un import sin usar tras
      // refactorizar) rompería el build en otra máquina sin aportar nada.
      "-Ywarn-unused:imports"
    ),

    Compile / run / fork := true,
    Test / parallelExecution := false,

    // imagen de contenedor: sbt docker:publishLocal
    dockerBaseImage := "eclipse-temurin:21-jre-jammy",
    dockerExposedPorts := Seq(8081),
    Docker / packageName := "bigdata-orchestrator"
  )
