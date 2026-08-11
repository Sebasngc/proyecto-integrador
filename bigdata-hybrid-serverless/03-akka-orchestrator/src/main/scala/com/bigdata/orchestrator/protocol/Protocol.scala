package com.bigdata.orchestrator.protocol

import java.time.Instant

/** =====================================================================
  * Protocolo del sistema de actores.
  *
  * Todo mensaje es inmutable y serializable (Jackson CBOR, ver
  * application.conf). Que el protocolo viva en un fichero aparte y sea
  * `sealed` da dos garantías importantes:
  *
  *  - el compilador avisa si un actor deja de tratar un caso (exhaustividad
  *    en el pattern matching), que es la fuente número uno de mensajes
  *    perdidos silenciosamente en sistemas de actores;
  *  - los tipos de `ActorRef[T]` documentan quién puede hablar con quién:
  *    un actor sólo puede recibir mensajes de su propio ADT.
  * ===================================================================== */

/** Marcador de serialización. `application.conf` enlaza este trait con
  * jackson-cbor: cualquier mensaje que pueda cruzar la frontera de un nodo debe
  * extenderlo. Sin este enlace explícito, Akka usaría serialización Java, que es
  * lenta y además insegura (deserializar datos no confiables permite ejecución
  * remota de código). */
trait JsonSerializable

// --------------------------------------------------------------- modelo ----

sealed trait JobStage { def name: String }
object JobStage {
  case object Received   extends JobStage { val name = "received"   }
  case object Validating extends JobStage { val name = "validating" }
  case object GpuPhase   extends JobStage { val name = "gpu"        }
  case object SparkPhase extends JobStage { val name = "spark"      }
  case object Analyzing  extends JobStage { val name = "analyzing"  }
  case object Responding extends JobStage { val name = "responding" }
  case object Completed  extends JobStage { val name = "completed"  }
  case object Failed     extends JobStage { val name = "failed"     }

  val all: Seq[JobStage] =
    Seq(Received, Validating, GpuPhase, SparkPhase, Analyzing, Responding, Completed, Failed)

  def fromName(s: String): JobStage = all.find(_.name == s).getOrElse(Failed)
}

/** Petición del usuario. `pipeline` = "rdd" | "dataframe" | "both". */
final case class JobRequest(
    datasetUri: String,
    mode: String = "zscore",
    pipeline: String = "both",
    outputUri: Option[String] = None,
    callbackUrl: Option[String] = None,
    forceCpu: Boolean = false,
    column: Option[String] = None
) extends JsonSerializable

final case class GpuResult(
    backend: String,
    device: String,
    elements: Long,
    mean: Double,
    stddev: Double,
    kernelMs: Double,
    wallMs: Double,
    outputUri: String
) extends JsonSerializable

final case class SparkRunResult(
    pipeline: String,
    runId: String,
    groups: Long,
    anomalies: Long,
    medianSeconds: Double,
    outputUri: Option[String]
) extends JsonSerializable

final case class Analysis(
    anomalyRate: Double,
    rddSeconds: Option[Double],
    dataFrameSeconds: Option[Double],
    sparkSpeedup: Option[Double],
    gpuBackend: String,
    totalSeconds: Double,
    verdict: String,
    warnings: List[String]
) extends JsonSerializable

final case class JobStatus(
    jobId: String,
    stage: String,
    createdAt: Instant,
    updatedAt: Instant,
    attempts: Map[String, Int],
    request: Option[JobRequest],
    gpu: Option[GpuResult],
    spark: List[SparkRunResult],
    analysis: Option[Analysis],
    error: Option[String]
) extends JsonSerializable

// ------------------------------------------------------------ comandos ----

/** Comandos aceptados por el orquestador de un job concreto. */
sealed trait JobCommand
object JobCommand {
  final case class Start(request: JobRequest, replyTo: akka.actor.typed.ActorRef[Ack]) extends JobCommand
  final case class GetStatus(replyTo: akka.actor.typed.ActorRef[Option[JobStatus]])    extends JobCommand
  final case class Cancel(reason: String)                                              extends JobCommand

  // Mensajes internos: los envían los actores de etapa a través de context.ask.
  private[orchestrator] final case class ValidationOk(normalized: JobRequest)      extends JobCommand
  private[orchestrator] final case class GpuOk(result: GpuResult)                  extends JobCommand
  private[orchestrator] final case class SparkOk(results: List[SparkRunResult])    extends JobCommand
  private[orchestrator] final case class AnalysisOk(analysis: Analysis)            extends JobCommand
  private[orchestrator] final case object ResponseDelivered                        extends JobCommand
  private[orchestrator] final case class StageFailed(stage: JobStage, reason: String, retryable: Boolean)
      extends JobCommand
  private[orchestrator] final case class RetryStage(stage: JobStage)               extends JobCommand
}

final case class Ack(jobId: String, accepted: Boolean, message: String) extends JsonSerializable

// ------------------------------------------------- comandos de las etapas ----

sealed trait ValidationCommand
object ValidationCommand {
  final case class Validate(jobId: String, request: JobRequest,
                            replyTo: akka.actor.typed.ActorRef[ValidationReply]) extends ValidationCommand
}
sealed trait ValidationReply
object ValidationReply {
  final case class Valid(normalized: JobRequest)            extends ValidationReply
  final case class Invalid(errors: List[String])            extends ValidationReply
}

sealed trait GpuCommand
object GpuCommand {
  final case class Preprocess(jobId: String, request: JobRequest,
                              replyTo: akka.actor.typed.ActorRef[GpuReply]) extends GpuCommand
}
sealed trait GpuReply
object GpuReply {
  final case class Done(result: GpuResult)                          extends GpuReply
  final case class Error(reason: String, retryable: Boolean)        extends GpuReply
}

sealed trait SparkCommand
object SparkCommand {
  final case class Submit(jobId: String, request: JobRequest, preprocessedUri: String,
                          replyTo: akka.actor.typed.ActorRef[SparkReply]) extends SparkCommand
  private[orchestrator] final case class Poll(jobId: String, runId: String, pipeline: String,
                                              attempt: Int) extends SparkCommand
}
sealed trait SparkReply
object SparkReply {
  final case class Done(results: List[SparkRunResult])       extends SparkReply
  final case class Error(reason: String, retryable: Boolean) extends SparkReply
}

sealed trait AnalyzerCommand
object AnalyzerCommand {
  final case class Analyze(jobId: String, gpu: GpuResult, spark: List[SparkRunResult],
                           elapsedSeconds: Double,
                           replyTo: akka.actor.typed.ActorRef[AnalyzerReply]) extends AnalyzerCommand
}
sealed trait AnalyzerReply
object AnalyzerReply {
  final case class Done(analysis: Analysis) extends AnalyzerReply
  final case class Error(reason: String)    extends AnalyzerReply
}

sealed trait ResponderCommand
object ResponderCommand {
  final case class Deliver(status: JobStatus, callbackUrl: Option[String],
                           replyTo: akka.actor.typed.ActorRef[ResponderReply]) extends ResponderCommand
}
sealed trait ResponderReply
object ResponderReply {
  case object Delivered                                    extends ResponderReply
  final case class Error(reason: String, retryable: Boolean) extends ResponderReply
}

// ----------------------------------------------------------- registro ----

sealed trait RegistryCommand
object RegistryCommand {
  final case class SubmitJob(request: JobRequest, replyTo: akka.actor.typed.ActorRef[Ack]) extends RegistryCommand
  final case class QueryJob(jobId: String, replyTo: akka.actor.typed.ActorRef[Option[JobStatus]]) extends RegistryCommand
  final case class ListJobs(replyTo: akka.actor.typed.ActorRef[List[JobStatus]])           extends RegistryCommand
  final case class CancelJob(jobId: String, reason: String)                                extends RegistryCommand
  private[orchestrator] final case class JobTerminated(jobId: String)                      extends RegistryCommand
}
