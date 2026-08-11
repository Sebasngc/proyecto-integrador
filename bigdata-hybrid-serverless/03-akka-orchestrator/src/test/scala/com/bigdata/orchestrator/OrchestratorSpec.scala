package com.bigdata.orchestrator

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

import com.bigdata.orchestrator.actors._
import com.bigdata.orchestrator.protocol._

/** =====================================================================
  * Tests del orquestador.
  *
  * El testkit de Akka permite verificar la máquina de estados SIN levantar
  * servicios reales: los actores de etapa se sustituyen por probes que
  * responden lo que el test decida. Así se pueden provocar de forma
  * determinista los escenarios difíciles de reproducir en vivo (fallo
  * transitorio del servicio GPU, entrada inválida, agotamiento de reintentos),
  * que es precisamente lo que hay que demostrar de un diseño con reintentos.
  * ===================================================================== */
class OrchestratorSpec
    extends ScalaTestWithActorTestKit(
      EventSourcedBehaviorTestKit.config.withFallback(ConfigFactory.load())
    )
    with AnyWordSpecLike {

  private val settings = JobOrchestrator.Settings(
    maxAttempts = 3, retryBaseDelay = 100.millis, stageTimeout = 3.seconds)

  private def newStages() = {
    val validation = TestProbe[ValidationCommand]()
    val gpu        = TestProbe[GpuCommand]()
    val spark      = TestProbe[SparkCommand]()
    val analyzer   = TestProbe[AnalyzerCommand]()
    val responder  = TestProbe[ResponderCommand]()
    (JobOrchestrator.Stages(validation.ref, gpu.ref, spark.ref, analyzer.ref, responder.ref),
     validation, gpu, spark, analyzer, responder)
  }

  private val request = JobRequest(datasetUri = "s3://bucket/data.parquet", mode = "zscore")

  private val gpuResult = GpuResult("cuda", "NVIDIA L4", 1000000L, 0.0, 1.0, 4.2, 120.0,
                                    "s3://bucket/processed/out.npy")

  "El orquestador de un job" should {

    "recorrer todas las etapas hasta completar" in {
      val (stages, validation, gpu, spark, analyzer, responder) = newStages()
      val job = spawn(JobOrchestrator("job-happy", stages, settings))
      val ack = TestProbe[Ack]()

      job ! JobCommand.Start(request, ack.ref)
      ack.expectMessageType[Ack].accepted shouldBe true

      // 1. validación
      val v = validation.expectMessageType[ValidationCommand.Validate]
      v.replyTo ! ValidationReply.Valid(request.copy(outputUri = Some("s3://bucket/out/")))

      // 2. GPU
      val g = gpu.expectMessageType[GpuCommand.Preprocess]
      g.replyTo ! GpuReply.Done(gpuResult)

      // 3. Spark: los dos pipelines
      val s = spark.expectMessageType[SparkCommand.Submit]
      s.replyTo ! SparkReply.Done(List(
        SparkRunResult("rdd", "r1", 5000L, 200L, 42.0, None),
        SparkRunResult("dataframe", "r2", 5000L, 200L, 14.0, None)))

      // 4. análisis
      val a = analyzer.expectMessageType[AnalyzerCommand.Analyze]
      a.replyTo ! AnalyzerReply.Done(
        Analysis(0.04, Some(42.0), Some(14.0), Some(3.0), "cuda", 60.0, "DataFrame gana", Nil))

      // 5. respuesta
      val r = responder.expectMessageType[ResponderCommand.Deliver]
      r.replyTo ! ResponderReply.Delivered

      val status = TestProbe[Option[JobStatus]]()
      eventually {
        job ! JobCommand.GetStatus(status.ref)
        status.expectMessageType[Option[JobStatus]].get.stage shouldBe JobStage.Completed.name
      }
    }

    "reintentar una etapa que falla de forma transitoria" in {
      val (stages, validation, gpu, _, _, _) = newStages()
      val job = spawn(JobOrchestrator("job-retry", stages, settings))
      val ack = TestProbe[Ack]()

      job ! JobCommand.Start(request, ack.ref)
      ack.expectMessageType[Ack]

      validation.expectMessageType[ValidationCommand.Validate].replyTo ! ValidationReply.Valid(request)

      // primer intento: fallo marcado como reintentable
      gpu.expectMessageType[GpuCommand.Preprocess].replyTo !
        GpuReply.Error("connection reset", retryable = true)

      // el orquestador debe volver a pedirlo tras el backoff
      val second = gpu.expectMessageType[GpuCommand.Preprocess](2.seconds)
      second.replyTo ! GpuReply.Done(gpuResult)

      val status = TestProbe[Option[JobStatus]]()
      eventually {
        job ! JobCommand.GetStatus(status.ref)
        val s = status.expectMessageType[Option[JobStatus]].get
        s.attempts.getOrElse(JobStage.GpuPhase.name, 0) shouldBe 1
      }
    }

    "NO reintentar una entrada inválida y fallar de inmediato" in {
      val (stages, validation, gpu, _, _, _) = newStages()
      val job = spawn(JobOrchestrator("job-invalid", stages, settings))
      val ack = TestProbe[Ack]()

      job ! JobCommand.Start(request.copy(datasetUri = "ftp://malo"), ack.ref)
      ack.expectMessageType[Ack]

      validation.expectMessageType[ValidationCommand.Validate].replyTo !
        ValidationReply.Invalid(List("esquema no soportado"))

      // clave: la fase GPU nunca debe llegar a invocarse
      gpu.expectNoMessage(1.second)

      val status = TestProbe[Option[JobStatus]]()
      eventually {
        job ! JobCommand.GetStatus(status.ref)
        status.expectMessageType[Option[JobStatus]].get.stage shouldBe JobStage.Failed.name
      }
    }

    "darse por vencido tras agotar maxAttempts" in {
      val (stages, validation, gpu, _, _, _) = newStages()
      val job = spawn(JobOrchestrator("job-exhausted", stages, settings))
      val ack = TestProbe[Ack]()

      job ! JobCommand.Start(request, ack.ref)
      ack.expectMessageType[Ack]
      validation.expectMessageType[ValidationCommand.Validate].replyTo ! ValidationReply.Valid(request)

      for (_ <- 1 to settings.maxAttempts + 1) {
        gpu.expectMessageType[GpuCommand.Preprocess](3.seconds).replyTo !
          GpuReply.Error("503 service unavailable", retryable = true)
      }

      val status = TestProbe[Option[JobStatus]]()
      eventually {
        job ! JobCommand.GetStatus(status.ref)
        val s = status.expectMessageType[Option[JobStatus]].get
        s.stage shouldBe JobStage.Failed.name
        s.error.get should include("agotados")
      }
    }

    "ser idempotente ante un Start duplicado" in {
      val (stages, validation, _, _, _, _) = newStages()
      val job = spawn(JobOrchestrator("job-dup", stages, settings))
      val ack = TestProbe[Ack]()

      job ! JobCommand.Start(request, ack.ref)
      ack.expectMessageType[Ack].accepted shouldBe true

      job ! JobCommand.Start(request, ack.ref)
      ack.expectMessageType[Ack].accepted shouldBe false

      validation.expectMessageType[ValidationCommand.Validate]
      validation.expectNoMessage(500.millis)  // no se ha duplicado el trabajo
    }
  }

  "El actor de validación" should {
    "aceptar una petición correcta y rellenar outputUri" in {
      val actor = spawn(ValidationActor())
      val probe = TestProbe[ValidationReply]()
      actor ! ValidationCommand.Validate("j1", request, probe.ref)
      probe.expectMessageType[ValidationReply.Valid].normalized.outputUri should not be empty
    }

    "rechazar modo y esquema inválidos acumulando todos los errores" in {
      val actor = spawn(ValidationActor())
      val probe = TestProbe[ValidationReply]()
      actor ! ValidationCommand.Validate("j2",
        JobRequest(datasetUri = "ftp://x", mode = "logaritmica", pipeline = "spark"), probe.ref)
      probe.expectMessageType[ValidationReply.Invalid].errors.size shouldBe 3
    }
  }

  "El analizador" should {
    "calcular el speedup y avisar si la GPU no se usó" in {
      val actor = spawn(ResultAnalyzerActor())
      val probe = TestProbe[AnalyzerReply]()
      actor ! AnalyzerCommand.Analyze("j3",
        gpuResult.copy(backend = "openmp"),
        List(SparkRunResult("rdd", "r1", 100L, 4L, 60.0, None),
             SparkRunResult("dataframe", "r2", 100L, 4L, 20.0, None)),
        90.0, probe.ref)

      val a = probe.expectMessageType[AnalyzerReply.Done].analysis
      a.sparkSpeedup.get shouldBe 3.0 +- 0.001
      a.warnings.exists(_.contains("CPU")) shouldBe true
    }
  }
}
