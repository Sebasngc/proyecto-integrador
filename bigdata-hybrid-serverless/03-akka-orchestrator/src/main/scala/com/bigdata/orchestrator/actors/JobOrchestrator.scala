package com.bigdata.orchestrator.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}

import java.time.Instant
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import com.bigdata.orchestrator.protocol._

/** =====================================================================
  * JobOrchestrator — una instancia por job. Es la máquina de estados que
  * encadena las etapas y el único punto donde vive la verdad sobre el job.
  *
  * Decisiones de diseño
  * --------------------
  * 1. **Un actor por job, no un actor por etapa compartido con estado.**
  *    El estado mutable del job queda encapsulado en su propio actor: no hay
  *    locks ni condiciones de carrera, y un job que falla no puede corromper
  *    a los demás (aislamiento de fallos del modelo de actores).
  *
  * 2. **Event sourcing (EventSourcedBehavior).** Se persisten *eventos*
  *    ("GpuCompleted"), no el estado. Si el nodo cae a mitad del job Spark,
  *    al recuperar se reconstruye el estado reproduciendo los eventos y se
  *    retoma desde la etapa alcanzada, sin repetir la fase GPU (que puede
  *    costar minutos de GPU facturada). Ese es exactamente el problema que
  *    "persistencia" resuelve aquí, y no un simple log.
  *
  * 3. **Reintentos en dos niveles.**
  *    - *Táctico*: dentro de cada actor de etapa, sobre errores transitorios
  *      de red (RetrySupport, backoff exponencial con jitter).
  *    - *Estratégico*: aquí. Si una etapa entera falla y es reintentable, se
  *      reprograma con `timers.startSingleTimer` hasta maxAttempts. El
  *      contador vive en el estado persistido, así que sobrevive a un reinicio.
  *
  * 4. **Supervisión con backoff** sobre fallos de persistencia: si el journal
  *    no está disponible, el actor se reinicia con espera creciente en vez de
  *    entrar en un bucle de fallo que tumbe el nodo.
  * ===================================================================== */
object JobOrchestrator {

  /** Referencias a los actores de etapa (inyectadas: facilita los tests con dobles). */
  final case class Stages(
      validation: ActorRef[ValidationCommand],
      gpu: ActorRef[GpuCommand],
      spark: ActorRef[SparkCommand],
      analyzer: ActorRef[AnalyzerCommand],
      responder: ActorRef[ResponderCommand]
  )

  final case class Settings(maxAttempts: Int, retryBaseDelay: FiniteDuration, stageTimeout: FiniteDuration)

  /** Techo del backoff: sin él, el intento 10 esperaría horas. */
  private val MaxBackoff: FiniteDuration = 2.minutes

  // ------------------------------------------------------------- eventos ----
  sealed trait Event
  object Event {
    final case class JobAccepted(request: JobRequest, at: Instant)   extends Event
    final case class InputValidated(normalized: JobRequest)          extends Event
    final case class GpuCompleted(result: GpuResult)                 extends Event
    final case class SparkCompleted(results: List[SparkRunResult])   extends Event
    final case class AnalysisCompleted(analysis: Analysis)           extends Event
    case object ResponseDelivered                                    extends Event
    final case class StageRetryScheduled(stage: String, attempt: Int, reason: String) extends Event
    final case class JobFailed(stage: String, reason: String)        extends Event
  }

  // -------------------------------------------------------------- estado ----
  final case class State(
      jobId: String,
      stage: JobStage,
      createdAt: Instant,
      updatedAt: Instant,
      attempts: Map[String, Int],
      request: Option[JobRequest],
      gpu: Option[GpuResult],
      spark: List[SparkRunResult],
      analysis: Option[Analysis],
      error: Option[String]
  ) {
    def toStatus: JobStatus =
      JobStatus(jobId, stage.name, createdAt, updatedAt, attempts, request, gpu, spark, analysis, error)

    def isTerminal: Boolean = stage == JobStage.Completed || stage == JobStage.Failed

    def attemptsFor(s: JobStage): Int = attempts.getOrElse(s.name, 0)

    def applyEvent(evt: Event): State = {
      val now = Instant.now()
      evt match {
        case Event.JobAccepted(req, at) =>
          copy(stage = JobStage.Validating, request = Some(req), createdAt = at, updatedAt = now)
        case Event.InputValidated(req) =>
          copy(stage = JobStage.GpuPhase, request = Some(req), updatedAt = now)
        case Event.GpuCompleted(r) =>
          copy(stage = JobStage.SparkPhase, gpu = Some(r), updatedAt = now)
        case Event.SparkCompleted(rs) =>
          copy(stage = JobStage.Analyzing, spark = rs, updatedAt = now)
        case Event.AnalysisCompleted(a) =>
          copy(stage = JobStage.Responding, analysis = Some(a), updatedAt = now)
        case Event.ResponseDelivered =>
          copy(stage = JobStage.Completed, updatedAt = now)
        case Event.StageRetryScheduled(stage, attempt, reason) =>
          copy(attempts = attempts.updated(stage, attempt),
               error = Some(s"reintento $attempt en $stage: $reason"), updatedAt = now)
        case Event.JobFailed(stage, reason) =>
          copy(stage = JobStage.Failed, error = Some(s"[$stage] $reason"), updatedAt = now)
      }
    }
  }

  object State {
    def empty(jobId: String): State =
      State(jobId, JobStage.Received, Instant.now(), Instant.now(),
            Map.empty, None, None, Nil, None, None)
  }

  // ----------------------------------------------------------- behavior ----
  def apply(jobId: String, stages: Stages, settings: Settings): Behavior[JobCommand] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { timers =>
        EventSourcedBehavior[JobCommand, Event, State](
          persistenceId = PersistenceId.ofUniqueId(s"job-$jobId"),
          emptyState = State.empty(jobId),
          commandHandler = (state, cmd) => onCommand(ctx, timers, stages, settings, state, cmd),
          eventHandler = (state, evt) => state.applyEvent(evt)
        ).withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 20, keepNSnapshots = 2))
         .onPersistFailure(
           SupervisorStrategy.restartWithBackoff(minBackoff = 200.millis, maxBackoff = 10.seconds, randomFactor = 0.2)
         )
      }
    }

  // ----------------------------------------------------- manejo de comandos ----
  private def onCommand(
      ctx: ActorContext[JobCommand],
      timers: TimerScheduler[JobCommand],
      stages: Stages,
      settings: Settings,
      state: State,
      cmd: JobCommand
  ): Effect[Event, State] = {
    import JobCommand._
    implicit val timeout: akka.util.Timeout = akka.util.Timeout(settings.stageTimeout)

    cmd match {

      case Start(request, replyTo) if state.stage == JobStage.Received =>
        ctx.log.info("job {} aceptado: dataset={} pipeline={}", state.jobId, request.datasetUri, request.pipeline)
        Effect
          .persist(Event.JobAccepted(request, Instant.now()))
          .thenRun((s: State) => runStage(ctx, stages, settings, s, JobStage.Validating))
          .thenReply(replyTo)(_ => Ack(state.jobId, accepted = true, "job aceptado"))

      case Start(_, replyTo) =>
        // Idempotencia: reenviar Start sobre un job existente no lo duplica.
        Effect.reply(replyTo)(Ack(state.jobId, accepted = false, s"el job ya está en '${state.stage.name}'"))

      case GetStatus(replyTo) =>
        Effect.reply(replyTo)(Some(state.toStatus))

      case ValidationOk(normalized) =>
        Effect
          .persist(Event.InputValidated(normalized))
          .thenRun((s: State) => runStage(ctx, stages, settings, s, JobStage.GpuPhase))

      case GpuOk(result) =>
        ctx.log.info("job {} GPU ok: backend={} n={} ({} ms)", state.jobId, result.backend,
                     result.elements, result.wallMs)
        Effect
          .persist(Event.GpuCompleted(result))
          .thenRun((s: State) => runStage(ctx, stages, settings, s, JobStage.SparkPhase))

      case SparkOk(results) =>
        Effect
          .persist(Event.SparkCompleted(results))
          .thenRun((s: State) => runStage(ctx, stages, settings, s, JobStage.Analyzing))

      case AnalysisOk(analysis) =>
        Effect
          .persist(Event.AnalysisCompleted(analysis))
          .thenRun((s: State) => runStage(ctx, stages, settings, s, JobStage.Responding))

      case ResponseDelivered =>
        ctx.log.info("job {} completado", state.jobId)
        Effect.persist(Event.ResponseDelivered)

      case StageFailed(stage, reason, retryable) =>
        val attempt = state.attemptsFor(stage) + 1
        if (retryable && attempt <= settings.maxAttempts) {
          // Backoff exponencial con jitter: si 500 jobs fallan a la vez por una
          // caída del servicio GPU, sin jitter todos reintentarían en el mismo
          // instante y lo tumbarían otra vez al recuperarse (thundering herd).
          val jitter = 0.75 + scala.util.Random.nextDouble() * 0.5
          val raw: FiniteDuration = settings.retryBaseDelay * math.pow(2, attempt - 1).toLong
          val jittered: FiniteDuration = (raw.toMillis * jitter).toLong.millis
          val capped: FiniteDuration = if (jittered > MaxBackoff) MaxBackoff else jittered
          ctx.log.warn("job {} etapa {} falló ({}); reintento {}/{} en {}",
                       state.jobId, stage.name, reason, attempt, settings.maxAttempts, capped)
          Effect
            .persist(Event.StageRetryScheduled(stage.name, attempt, reason))
            .thenRun((_: State) => timers.startSingleTimer(s"retry-${stage.name}", RetryStage(stage), capped))
        } else {
          val why = if (retryable) s"agotados ${settings.maxAttempts} intentos: $reason" else reason
          ctx.log.error("job {} falló definitivamente en {}: {}", state.jobId, stage.name, why)
          Effect
            .persist(Event.JobFailed(stage.name, why))
            .thenRun((s: State) => notifyFailure(ctx, stages, settings, s))
        }

      case RetryStage(stage) =>
        Effect.none.thenRun((s: State) => runStage(ctx, stages, settings, s, stage))

      case Cancel(reason) if !state.isTerminal =>
        Effect.persist(Event.JobFailed(state.stage.name, s"cancelado: $reason"))

      case Cancel(_) =>
        Effect.none
    }
  }

  // ------------------------------------------- disparo de la etapa siguiente ----
  /** Efecto lateral: pide a un actor de etapa que trabaje y traduce su respuesta
    * a un comando interno. `ctx.ask` es la forma segura de hacerlo: la respuesta
    * (o el timeout) llega como un mensaje normal al buzón, nunca como un
    * callback ejecutándose en otro hilo sobre el estado del actor. */
  private def runStage(
      ctx: ActorContext[JobCommand],
      stages: Stages,
      settings: Settings,
      state: State,
      stage: JobStage
  ): Unit = {
    import JobCommand._
    implicit val timeout: akka.util.Timeout = akka.util.Timeout(settings.stageTimeout)
    val request = state.request

    stage match {
      case JobStage.Validating =>
        request.foreach { req =>
          ctx.ask(stages.validation, (r: ActorRef[ValidationReply]) =>
            ValidationCommand.Validate(state.jobId, req, r)) {
            case Success(ValidationReply.Valid(norm))    => ValidationOk(norm)
            case Success(ValidationReply.Invalid(errs))  =>
              // entrada inválida = error del cliente: NO se reintenta
              StageFailed(JobStage.Validating, errs.mkString("; "), retryable = false)
            case Failure(ex)                             =>
              StageFailed(JobStage.Validating, ex.getMessage, retryable = true)
          }
        }

      case JobStage.GpuPhase =>
        request.foreach { req =>
          ctx.ask(stages.gpu, (r: ActorRef[GpuReply]) =>
            GpuCommand.Preprocess(state.jobId, req, r)) {
            case Success(GpuReply.Done(res))            => GpuOk(res)
            case Success(GpuReply.Error(why, retry))    => StageFailed(JobStage.GpuPhase, why, retry)
            case Failure(ex)                            => StageFailed(JobStage.GpuPhase, ex.getMessage, retryable = true)
          }
        }

      case JobStage.SparkPhase =>
        (request, state.gpu) match {
          case (Some(req), Some(gpu)) =>
            ctx.ask(stages.spark, (r: ActorRef[SparkReply]) =>
              SparkCommand.Submit(state.jobId, req, gpu.outputUri, r)) {
              case Success(SparkReply.Done(rs))         => SparkOk(rs)
              case Success(SparkReply.Error(why, retry))=> StageFailed(JobStage.SparkPhase, why, retry)
              case Failure(ex)                          => StageFailed(JobStage.SparkPhase, ex.getMessage, retryable = true)
            }
          case _ =>
            ctx.self ! StageFailed(JobStage.SparkPhase, "falta el resultado de la fase GPU", retryable = false)
        }

      case JobStage.Analyzing =>
        state.gpu.foreach { gpu =>
          val elapsed = (Instant.now().toEpochMilli - state.createdAt.toEpochMilli) / 1000.0
          ctx.ask(stages.analyzer, (r: ActorRef[AnalyzerReply]) =>
            AnalyzerCommand.Analyze(state.jobId, gpu, state.spark, elapsed, r)) {
            case Success(AnalyzerReply.Done(a))    => AnalysisOk(a)
            case Success(AnalyzerReply.Error(why)) => StageFailed(JobStage.Analyzing, why, retryable = true)
            case Failure(ex)                       => StageFailed(JobStage.Analyzing, ex.getMessage, retryable = true)
          }
        }

      case JobStage.Responding =>
        ctx.ask(stages.responder, (r: ActorRef[ResponderReply]) =>
          ResponderCommand.Deliver(state.toStatus, request.flatMap(_.callbackUrl), r)) {
          case Success(ResponderReply.Delivered)        => ResponseDelivered
          case Success(ResponderReply.Error(why, retry))=> StageFailed(JobStage.Responding, why, retry)
          case Failure(ex)                              => StageFailed(JobStage.Responding, ex.getMessage, retryable = true)
        }

      case other =>
        ctx.log.debug("no hay acción para la etapa {}", other.name)
    }
  }

  /** Aun fallando, el cliente merece saberlo: se intenta entregar el estado final. */
  private def notifyFailure(ctx: ActorContext[JobCommand], stages: Stages,
                            settings: Settings, state: State): Unit = {
    implicit val timeout: akka.util.Timeout = akka.util.Timeout(settings.stageTimeout)
    state.request.flatMap(_.callbackUrl).foreach { _ =>
      ctx.ask(stages.responder, (r: ActorRef[ResponderReply]) =>
        ResponderCommand.Deliver(state.toStatus, state.request.flatMap(_.callbackUrl), r)) {
        case _ => JobCommand.Cancel("notificación de fallo entregada")
      }
    }
  }
}
