package com.bigdata.orchestrator

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior, SupervisorStrategy}
import akka.http.scaladsl.Http
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success}

import com.bigdata.orchestrator.actors._
import com.bigdata.orchestrator.http.HttpRoutes
import com.bigdata.orchestrator.repo.{DynamoJobRepository, InMemoryJobRepository, JobRepository}

/** =====================================================================
  * Punto de entrada del orquestador.
  *
  * Ejecutar:
  *   sbt run                                   # perfil local (repo en memoria)
  *   sbt -Dconfig.resource=production.conf run # DynamoDB + endpoints reales
  *
  * Nota sobre la licencia de Akka: desde la versión 2.7, Akka usa la Business
  * Source License (gratuita por debajo de cierto umbral de facturación, de pago
  * por encima). **Apache Pekko** es el fork Apache 2.0 de la última versión
  * Apache de Akka: el código de este proyecto migra sustituyendo `akka.` por
  * `org.apache.pekko.` en los imports y las dependencias en build.sbt. En
  * build.sbt está el bloque comentado para hacerlo.
  * ===================================================================== */
object Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem[Nothing](Guardian(), "bigdata-orchestrator")
    // El sistema se apaga solo desde CoordinatedShutdown; este hook sólo asegura
    // que un SIGTERM del contenedor drene las peticiones en vuelo.
    sys.addShutdownHook {
      system.terminate()
      scala.concurrent.Await.result(system.whenTerminated, 30.seconds)
      ()
    }
  }
}

object Guardian {

  def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
    implicit val system: ActorSystem[_] = ctx.system
    implicit val ec: scala.concurrent.ExecutionContext = ctx.executionContext

    val cfg = ConfigFactory.load().getConfig("orchestrator")

    // ------------------------------- repositorio -------------------------------
    val repo: JobRepository = cfg.getString("repository.type") match {
      case "dynamodb" => new DynamoJobRepository(cfg.getString("repository.table"))
      case _          => new InMemoryJobRepository()
    }

    // --------------------------- actores de etapa ------------------------------
    // Cada uno se envuelve en supervisión con backoff: un fallo aislado no
    // derriba el sistema, y el reinicio espaciado evita el bucle de fallo.
    def supervised[T](b: Behavior[T], name: String) =
      ctx.spawn(
        Behaviors.supervise(b).onFailure[Exception](
          SupervisorStrategy.restartWithBackoff(200.millis, 10.seconds, 0.2)
        ),
        name
      )

    val validation = supervised(ValidationActor(), "validation")

    val gpu = supervised(
      GpuPreprocessActor(GpuPreprocessActor.Config(
        endpoint   = cfg.getString("gpu.endpoint"),
        timeout    = cfg.getDuration("gpu.timeout").toMillis.millis,
        maxRetries = cfg.getInt("gpu.max-retries")
      )),
      "gpu-preprocess"
    )

    val spark = supervised(
      SparkJobActor(SparkJobActor.Config(
        launcherEndpoint = cfg.getString("spark.launcher-endpoint"),
        pollInterval     = cfg.getDuration("spark.poll-interval").toMillis.millis,
        maxPolls         = cfg.getInt("spark.max-polls"),
        timeout          = cfg.getDuration("spark.timeout").toMillis.millis,
        maxRetries       = cfg.getInt("spark.max-retries")
      )),
      "spark-job"
    )

    val analyzer  = supervised(ResultAnalyzerActor(), "analyzer")
    val responder = supervised(ResponderActor(repo), "responder")

    val stages = JobOrchestrator.Stages(validation, gpu, spark, analyzer, responder)
    val settings = JobOrchestrator.Settings(
      maxAttempts    = cfg.getInt("job.max-attempts"),
      retryBaseDelay = cfg.getDuration("job.retry-base-delay").toMillis.millis,
      stageTimeout   = cfg.getDuration("job.stage-timeout").toMillis.millis
    )

    val registry = ctx.spawn(JobRegistry(stages, settings, repo), "job-registry")

    // ---------------------------------- HTTP -----------------------------------
    val host = cfg.getString("http.host")
    val port = cfg.getInt("http.port")
    val routes = new HttpRoutes(registry).routes

    
Http().newServerAt("0.0.0.0", 8081).bind(routes).onComplete {
  case Success(binding) =>
    system.log.info(s"Server online at ${binding.localAddress}")
  case Failure(ex) =>
    system.log.error(s"Failed to bind HTTP endpoint", ex)
    system.terminate()
}

    Behaviors.empty
  }
}
