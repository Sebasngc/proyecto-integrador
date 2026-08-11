package com.bigdata.orchestrator.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.pattern.CircuitBreaker

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import com.bigdata.orchestrator.protocol._
import com.bigdata.orchestrator.util.{Json, RetrySupport}

/** =====================================================================
  * Actores de etapa. Cada uno tiene UNA responsabilidad, tal y como pide el
  * enunciado: validación de entrada, emisión del job, análisis de resultados
  * y respuesta.
  *
  * Todos se crean con `Behaviors.supervise(...).onFailure(restartWithBackoff)`
  * en Guardian: si uno revienta por una excepción inesperada, se reinicia
  * limpio con espera creciente en vez de propagar el fallo hacia arriba y
  * tumbar el sistema. Es la diferencia práctica entre "let it crash" bien
  * aplicado y un try/catch gigante.
  * ===================================================================== */

// ---------------------------------------------------------------------------
// 1. Validación de entrada — actor puro, sin E/S, determinista y trivial de testear
// ---------------------------------------------------------------------------
object ValidationActor {

  private val ValidModes     = Set("minmax", "zscore", "robust")
  private val ValidPipelines = Set("rdd", "dataframe", "both")
  private val UriPattern     = "^(s3|s3a|gs|hdfs|file|abfss)://.+".r

  def apply(): Behavior[ValidationCommand] = Behaviors.receive { (ctx, msg) =>
    msg match {
      case ValidationCommand.Validate(jobId, req, replyTo) =>
        val errors = scala.collection.mutable.ListBuffer.empty[String]

        if (req.datasetUri.isEmpty) errors += "datasetUri es obligatorio"
        else if (UriPattern.findFirstIn(req.datasetUri).isEmpty)
          errors += s"datasetUri con esquema no soportado: ${req.datasetUri}"

        if (!ValidModes.contains(req.mode))
          errors += s"mode '${req.mode}' inválido (use ${ValidModes.mkString("/")})"
        if (!ValidPipelines.contains(req.pipeline))
          errors += s"pipeline '${req.pipeline}' inválido (use ${ValidPipelines.mkString("/")})"

        req.callbackUrl.foreach { url =>
          if (!url.startsWith("https://") && !url.startsWith("http://"))
            errors += s"callbackUrl debe ser http(s): $url"
        }

        if (errors.isEmpty) {
          // Normalización: rellenar el destino por defecto evita que cada etapa
          // posterior tenga que reinventar la convención de rutas.
          val normalized = req.copy(
            mode = req.mode.toLowerCase,
            pipeline = req.pipeline.toLowerCase,
            outputUri = req.outputUri.orElse(Some(defaultOutput(req.datasetUri, jobId)))
          )
          ctx.log.info("job {} validado", jobId)
          replyTo ! ValidationReply.Valid(normalized)
        } else {
          ctx.log.warn("job {} inválido: {}", jobId, errors.mkString("; "))
          replyTo ! ValidationReply.Invalid(errors.toList)
        }
        Behaviors.same
    }
  }

  private def defaultOutput(datasetUri: String, jobId: String): String = {
    val base = datasetUri.reverse.dropWhile(_ != '/').reverse
    s"${base}processed/$jobId/"
  }
}

// ---------------------------------------------------------------------------
// 2. Emisión de la fase GPU — llama al microservicio serverless
// ---------------------------------------------------------------------------
object GpuPreprocessActor {

  final case class Config(endpoint: String, timeout: FiniteDuration, maxRetries: Int)

  def apply(cfg: Config): Behavior[GpuCommand] = Behaviors.setup { ctx =>
    implicit val system: akka.actor.typed.ActorSystem[_] = ctx.system
    implicit val ec: ExecutionContext = ctx.executionContext

    // Circuit breaker: tras 5 fallos consecutivos se abre durante 30 s y
    // rechaza inmediatamente, sin gastar 5 reintentos * timeout por cada job.
    // Protege al servicio GPU de recibir tráfico mientras se recupera y evita
    // que los jobs se queden colgados esperando timeouts.
    val breaker = CircuitBreaker(
      scheduler = ctx.system.classicSystem.scheduler,
      maxFailures = 5,
      callTimeout = cfg.timeout,
      resetTimeout = 30.seconds
    ).onOpen(ctx.log.error("circuito ABIERTO hacia el servicio GPU {}", cfg.endpoint))
     .onHalfOpen(ctx.log.info("circuito medio-abierto: probando el servicio GPU"))
     .onClose(ctx.log.info("circuito cerrado: servicio GPU recuperado"))

    Behaviors.receiveMessage {
      case GpuCommand.Preprocess(jobId, req, replyTo) =>
        val payload = Json.obj(
          "request_id" -> jobId,
          "input_uri"  -> req.datasetUri,
          "output_uri" -> req.outputUri.getOrElse(s"${req.datasetUri}.norm.npy"),
          "mode"       -> req.mode,
          "force_cpu"  -> req.forceCpu,
          "column"     -> req.column.getOrElse("value")
        )

        val call: () => Future[GpuResult] = () =>
          breaker.withCircuitBreaker(
            Http()
              .singleRequest(HttpRequest(
                method = HttpMethods.POST,
                uri = s"/preprocess",
                entity = HttpEntity(ContentTypes.`application/json`, payload)
              ))
              .flatMap { resp =>
                Unmarshal(resp.entity).to[String].map { body =>
                  if (resp.status.isSuccess()) parseGpu(body, req)
                  else throw new RuntimeException(s"servicio GPU HTTP ${resp.status.intValue()}: ${body.take(300)}")
                }
              }
          )

        // Reintento táctico dentro de la etapa: sólo para errores transitorios.
        RetrySupport
          .retry(call, cfg.maxRetries, 300.millis, ctx.system)
          .onComplete {
            case Success(res) => replyTo ! GpuReply.Done(res)
            case Failure(ex)  => replyTo ! GpuReply.Error(ex.getMessage, retryable = RetrySupport.isRetryable(ex))
          }

        Behaviors.same
    }
  }

  private def parseGpu(body: String, req: JobRequest): GpuResult = {
    val j = Json.parse(body)
    GpuResult(
      backend  = j.str("backend").getOrElse("unknown"),
      device   = j.str("device").getOrElse("unknown"),
      elements = j.long("elements").getOrElse(0L),
      mean     = j.nested("stats").flatMap(_.double("mean")).getOrElse(Double.NaN),
      stddev   = j.nested("stats").flatMap(_.double("stddev")).getOrElse(Double.NaN),
      kernelMs = j.nested("timing_ms").flatMap(_.double("kernel_ms")).getOrElse(0.0),
      wallMs   = j.double("wall_ms").getOrElse(0.0),
      outputUri = j.str("output_uri").orElse(req.outputUri).getOrElse("")
    )
  }
}

// ---------------------------------------------------------------------------
// 3. Emisión del job Spark — lanza y sondea hasta terminación
// ---------------------------------------------------------------------------
object SparkJobActor {

  final case class Config(
      launcherEndpoint: String,     // Lambda / EMR Serverless / spark-submit REST
      pollInterval: FiniteDuration,
      maxPolls: Int,
      timeout: FiniteDuration,
      maxRetries: Int
  )

  def apply(cfg: Config): Behavior[SparkCommand] = Behaviors.setup { ctx =>
    implicit val system: akka.actor.typed.ActorSystem[_] = ctx.system
    implicit val ec: ExecutionContext = ctx.executionContext

    Behaviors.receiveMessage {
      case SparkCommand.Submit(jobId, req, preprocessedUri, replyTo) =>
        val pipelines = if (req.pipeline == "both") List("rdd", "dataframe") else List(req.pipeline)

        // Los dos pipelines se lanzan en PARALELO cuando pipeline="both": son
        // independientes y así el tiempo de pared es el del más lento, no la
        // suma. Con Future.sequence, si uno falla falla el conjunto, que es la
        // semántica que queremos (no se puede calcular speedup con uno solo).
        val futures = pipelines.map { p =>
          val payload = Json.obj(
            "job_id"    -> jobId,
            "pipeline"  -> p,
            "input_uri" -> preprocessedUri,
            "output_uri"-> req.outputUri.map(_ + p).getOrElse(""),
            "runs"      -> 1
          )
          RetrySupport.retry(
            () => submitAndWait(cfg, payload, p, jobId),
            cfg.maxRetries, 1.second, ctx.system
          )
        }

        Future.sequence(futures).onComplete {
          case Success(results) => replyTo ! SparkReply.Done(results)
          case Failure(ex)      => replyTo ! SparkReply.Error(ex.getMessage, RetrySupport.isRetryable(ex))
        }
        Behaviors.same

      case _: SparkCommand.Poll =>
        Behaviors.same
    }
  }

  /** Lanza el job y sondea su estado. El sondeo se hace con Futures encadenados
    * y no bloquea ningún hilo del dispatcher del actor. */
  private def submitAndWait(cfg: Config, payload: String, pipeline: String, jobId: String)(implicit
      system: akka.actor.typed.ActorSystem[_], ec: ExecutionContext): Future[SparkRunResult] = {

    def post(path: String, body: String): Future[String] =
      Http().singleRequest(HttpRequest(
        method = HttpMethods.POST,
        uri = s"${cfg.launcherEndpoint}$path",
        entity = HttpEntity(ContentTypes.`application/json`, body)
      )).flatMap { r =>
        Unmarshal(r.entity).to[String].map { b =>
          if (r.status.isSuccess()) b
          else throw new RuntimeException(s"lanzador Spark HTTP ${r.status.intValue()}: ${b.take(300)}")
        }
      }

    def poll(runId: String, remaining: Int): Future[SparkRunResult] = {
      if (remaining <= 0)
        Future.failed(new RuntimeException(s"job Spark $runId sin terminar tras ${cfg.maxPolls} sondeos"))
      else
        post("/status", Json.obj("run_id" -> runId)).flatMap { body =>
          val j = Json.parse(body)
          j.str("state").getOrElse("PENDING").toUpperCase match {
            case "SUCCESS" | "COMPLETED" =>
              Future.successful(SparkRunResult(
                pipeline = pipeline,
                runId = runId,
                groups = j.long("groups").getOrElse(0L),
                anomalies = j.long("anomalies").getOrElse(0L),
                medianSeconds = j.double("median_s").getOrElse(0.0),
                outputUri = j.str("output_uri")
              ))
            case "FAILED" | "CANCELLED" =>
              Future.failed(new RuntimeException(
                s"job Spark $runId terminó en ${j.str("state").getOrElse("?")}: " +
                  j.str("error").getOrElse("sin detalle")))
            case _ =>
              akka.pattern.after(cfg.pollInterval)(poll(runId, remaining - 1))(system.classicSystem)
          }
        }
    }

    post("/submit", payload).flatMap { body =>
      val runId = Json.parse(body).str("run_id")
        .getOrElse(throw new RuntimeException("el lanzador no devolvió run_id"))
      poll(runId, cfg.maxPolls)
    }
  }
}

// ---------------------------------------------------------------------------
// 4. Análisis de resultados — cálculo del speedup y del veredicto
// ---------------------------------------------------------------------------
object ResultAnalyzerActor {

  def apply(): Behavior[AnalyzerCommand] = Behaviors.receive { (ctx, msg) =>
    msg match {
      case AnalyzerCommand.Analyze(jobId, gpu, spark, elapsed, replyTo) =>
        Try {
          val rdd = spark.find(_.pipeline == "rdd").map(_.medianSeconds)
          val df  = spark.find(_.pipeline == "dataframe").map(_.medianSeconds)
          val speedup = for { r <- rdd; d <- df if d > 0 } yield r / d

          val totalReadings = spark.map(_.groups).maxOption.getOrElse(0L)
          val anomalies     = spark.map(_.anomalies).maxOption.getOrElse(0L)
          val rate          = if (totalReadings > 0) anomalies.toDouble / totalReadings else 0.0

          val warnings = scala.collection.mutable.ListBuffer.empty[String]
          if (gpu.backend != "cuda")
            warnings += "la fase GPU se ejecutó en CPU (OpenMP): el runtime no expuso ninguna GPU"
          // Discrepancia entre pipelines = bug, no ruido de medición: se avisa fuerte.
          if (spark.map(_.groups).distinct.size > 1)
            warnings += "los pipelines RDD y DataFrame devolvieron cardinalidades distintas"
          if (rate > 0.05)
            warnings += f"tasa de anomalías inusualmente alta (${rate * 100}%.1f%%): revise la calibración del sensor"

          val verdict = speedup match {
            case Some(s) if s >= 1.5 => f"DataFrame es $s%.2fx más rápido que RDD"
            case Some(s) if s >= 1.0 => f"DataFrame es marginalmente más rápido ($s%.2fx)"
            case Some(s)             => f"RDD resultó más rápido (${1 / s}%.2fx): revise el tamaño del dataset"
            case None                => "sólo se ejecutó un pipeline: sin comparación disponible"
          }

          Analysis(rate, rdd, df, speedup, gpu.backend, elapsed, verdict, warnings.toList)
        } match {
          case Success(a) =>
            ctx.log.info("job {} analizado: {}", jobId, a.verdict)
            replyTo ! AnalyzerReply.Done(a)
          case Failure(ex) =>
            replyTo ! AnalyzerReply.Error(ex.getMessage)
        }
        Behaviors.same
    }
  }
}

// ---------------------------------------------------------------------------
// 5. Respuesta final — persistencia del resultado y webhook opcional
// ---------------------------------------------------------------------------
object ResponderActor {

  def apply(repo: com.bigdata.orchestrator.repo.JobRepository): Behavior[ResponderCommand] =
    Behaviors.setup { ctx =>
      implicit val system: akka.actor.typed.ActorSystem[_] = ctx.system
      implicit val ec: ExecutionContext = ctx.executionContext

      Behaviors.receiveMessage {
        case ResponderCommand.Deliver(status, callbackUrl, replyTo) =>
          // Se persiste SIEMPRE, aunque el webhook falle: el resultado debe
          // quedar consultable por GET /jobs/{id} pase lo que pase.
          val persisted = repo.save(status)

          val delivered = callbackUrl match {
            case None => persisted.map(_ => ())
            case Some(url) =>
              persisted.flatMap { _ =>
                RetrySupport.retry(
                  () => Http().singleRequest(HttpRequest(
                    method = HttpMethods.POST,
                    uri = url,
                    entity = HttpEntity(ContentTypes.`application/json`, Json.encodeStatus(status))
                  )).flatMap { r =>
                    if (r.status.isSuccess()) { r.discardEntityBytes(); Future.successful(()) }
                    else {
                      r.discardEntityBytes()
                      Future.failed(new RuntimeException(s"webhook devolvió ${r.status.intValue()}"))
                    }
                  },
                  maxAttempts = 3, base = 500.millis, system = ctx.system
                )
              }
          }

          delivered.onComplete {
            case Success(_)  => replyTo ! ResponderReply.Delivered
            case Failure(ex) => replyTo ! ResponderReply.Error(ex.getMessage, RetrySupport.isRetryable(ex))
          }
          Behaviors.same
      }
    }
}
