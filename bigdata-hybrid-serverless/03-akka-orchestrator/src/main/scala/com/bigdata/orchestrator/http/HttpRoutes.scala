package com.bigdata.orchestrator.http

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, MalformedRequestContentRejection, RejectionHandler, Route}
import akka.util.Timeout
import spray.json._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

import com.bigdata.orchestrator.protocol._

/** =====================================================================
  * API HTTP. Contrato asíncrono en dos tiempos, que es lo correcto cuando el
  * trabajo dura minutos:
  *
  *   POST /api/v1/jobs           -> 202 Accepted + jobId   (no bloquea)
  *   GET  /api/v1/jobs/{id}      -> estado y etapa actual
  *   GET  /api/v1/jobs/{id}/result -> 200 con el análisis, o 409 si no ha
  *                                   terminado (el cliente hace polling)
  *   GET  /api/v1/jobs           -> últimos jobs
  *   DELETE /api/v1/jobs/{id}    -> cancelación
  *   GET  /health, /ready        -> sondas para el balanceador / Kubernetes
  *
  * Alternativa al polling: `callbackUrl` en el cuerpo de la petición; el
  * ResponderActor hace POST del resultado ahí en cuanto está.
  * ===================================================================== */
object JsonFormats extends DefaultJsonProtocol {
  import spray.json._

  implicit object InstantFormat extends RootJsonFormat[java.time.Instant] {
    def write(i: java.time.Instant): JsValue = JsString(i.toString)
    def read(v: JsValue): java.time.Instant = v match {
      case JsString(s) => java.time.Instant.parse(s)
      case other       => deserializationError(s"instante esperado, recibido $other")
    }
  }

  /** Formato escrito a mano en lugar de `jsonFormat7`.
    *
    * spray-json NO respeta los valores por defecto de los parámetros de Scala:
    * su macro exige que todo campo que no sea `Option` venga presente en el
    * JSON. `JobRequest.forceCpu` tiene default `false`, así que un cuerpo
    * perfectamente razonable como
    *
    *     {"datasetUri": "...", "mode": "zscore", "pipeline": "both"}
    *
    * era rechazado por un campo que el cliente no tiene por qué conocer. Aquí
    * los defaults se aplican de verdad y sólo `datasetUri` es obligatorio. */
  implicit object JobRequestFormat extends RootJsonFormat[JobRequest] {
    def write(r: JobRequest): JsValue = {
      val base = Map(
        "datasetUri" -> JsString(r.datasetUri),
        "mode"       -> JsString(r.mode),
        "pipeline"   -> JsString(r.pipeline),
        "forceCpu"   -> JsBoolean(r.forceCpu)
      )
      val opcionales = Map(
        "outputUri"   -> r.outputUri,
        "callbackUrl" -> r.callbackUrl,
        "column"      -> r.column
      ).collect { case (k, Some(v)) => k -> JsString(v) }
      JsObject(base ++ opcionales)
    }

    def read(v: JsValue): JobRequest = {
      val campos = v.asJsObject("se esperaba un objeto JSON").fields

      def texto(clave: String): Option[String] = campos.get(clave).flatMap {
        case JsString(s) if s.trim.nonEmpty => Some(s.trim)
        case JsNull                         => None
        case otro                           =>
          deserializationError(s"'$clave' debe ser una cadena, recibido $otro")
      }

      val datasetUri = texto("datasetUri").getOrElse(
        deserializationError("falta el campo obligatorio 'datasetUri'"))

      JobRequest(
        datasetUri  = datasetUri,
        mode        = texto("mode").getOrElse("zscore"),
        pipeline    = texto("pipeline").getOrElse("both"),
        outputUri   = texto("outputUri"),
        callbackUrl = texto("callbackUrl"),
        forceCpu    = campos.get("forceCpu").fold(false) {
                        case JsBoolean(b) => b
                        case JsNull       => false
                        case otro         =>
                          deserializationError(s"'forceCpu' debe ser booleano, recibido $otro")
                      },
        column      = texto("column")
      )
    }
  }

  implicit val gpuResultFormat: RootJsonFormat[GpuResult]         = jsonFormat8(GpuResult.apply)
  implicit val sparkRunFormat: RootJsonFormat[SparkRunResult]     = jsonFormat6(SparkRunResult.apply)
  implicit val analysisFormat: RootJsonFormat[Analysis]           = jsonFormat8(Analysis.apply)
  implicit val jobStatusFormat: RootJsonFormat[JobStatus]         = jsonFormat10(JobStatus.apply)
  implicit val ackFormat: RootJsonFormat[Ack]                     = jsonFormat3(Ack.apply)
}

final class HttpRoutes(registry: ActorRef[RegistryCommand])(implicit system: ActorSystem[_])
    extends SprayJsonSupport {

  import JsonFormats._

  private implicit val timeout: Timeout = Timeout(10.seconds)
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext

  /** Los errores no controlados devuelven 500 con un cuerpo JSON estable, nunca
    * una traza de Scala: filtrar detalles internos evita filtrar rutas, nombres
    * de bucket y versiones de librería a un cliente no confiable. */
  private val exceptionHandler = ExceptionHandler { case ex =>
    extractUri { uri =>
      system.log.error(s"fallo procesando $uri", ex)
      complete(StatusCodes.InternalServerError,
        JsObject("error" -> JsString("internal_error"),
                 "message" -> JsString("error interno; consulte los logs con el request-id")))
    }
  }

  /** El motivo del rechazo VIAJA en la respuesta. La versión anterior devolvía
    * un escueto `{"error":"bad_request"}` que no decía qué campo estaba mal,
    * convirtiendo un error de una línea en una sesión de depuración a ciegas.
    * Aquí no hay riesgo de filtrar nada: el texto describe el cuerpo que acaba
    * de enviar el propio cliente. */
  private val rejectionHandler = RejectionHandler.newBuilder()
    .handle {
      case MalformedRequestContentRejection(msg, _) =>
        complete(StatusCodes.BadRequest,
          JsObject("error" -> JsString("bad_request"), "message" -> JsString(msg)))
    }
    .handleAll[akka.http.scaladsl.server.Rejection] { rejections =>
      val detalle = rejections.map(_.toString).mkString("; ")
      complete(StatusCodes.BadRequest,
        JsObject("error" -> JsString("bad_request"), "message" -> JsString(detalle)))
    }
    .handleNotFound {
      complete(StatusCodes.NotFound,
        JsObject("error" -> JsString("not_found"), "message" -> JsString("ruta desconocida")))
    }
    .result()

  val routes: Route =
    handleExceptions(exceptionHandler) {
      handleRejections(rejectionHandler) {
        concat(
          pathPrefix("api" / "v1" / "jobs") {
            concat(
              pathEnd {
                concat(
                  post {
                    entity(as[JobRequest]) { req =>
                      onComplete(registry.ask[Ack](RegistryCommand.SubmitJob(req, _))) {
                        case Success(ack) if ack.accepted =>
                          respondWithHeader(
                            akka.http.scaladsl.model.headers.Location(s"/api/v1/jobs/${ack.jobId}")
                          ) { complete(StatusCodes.Accepted, ack) }
                        case Success(ack) => complete(StatusCodes.Conflict, ack)
                        case Failure(ex)  =>
                          complete(StatusCodes.ServiceUnavailable,
                            JsObject("error" -> JsString("registry_unavailable"),
                                     "message" -> JsString(ex.getMessage)))
                      }
                    }
                  },
                  get {
                    onSuccess(registry.ask[List[JobStatus]](RegistryCommand.ListJobs)) { jobs =>
                      complete(StatusCodes.OK, jobs)
                    }
                  }
                )
              },
              path(Segment) { jobId =>
                concat(
                  get {
                    onSuccess(registry.ask[Option[JobStatus]](RegistryCommand.QueryJob(jobId, _))) {
                      case Some(status) => complete(StatusCodes.OK, status)
                      case None =>
                        complete(StatusCodes.NotFound,
                          JsObject("error" -> JsString("not_found"), "job_id" -> JsString(jobId)))
                    }
                  },
                  delete {
                    registry ! RegistryCommand.CancelJob(jobId, "cancelado por el cliente")
                    complete(StatusCodes.Accepted,
                      JsObject("job_id" -> JsString(jobId), "status" -> JsString("cancelling")))
                  }
                )
              },
              path(Segment / "result") { jobId =>
                get {
                  onSuccess(registry.ask[Option[JobStatus]](RegistryCommand.QueryJob(jobId, _))) {
                    case Some(s) if s.stage == JobStage.Completed.name =>
                      complete(StatusCodes.OK, s)
                    case Some(s) if s.stage == JobStage.Failed.name =>
                      complete(StatusCodes.UnprocessableEntity, s)
                    case Some(s) =>
                      // 409 + Retry-After: el cliente sabe cuándo volver a preguntar
                      // sin martillear la API.
                      respondWithHeader(
                        akka.http.scaladsl.model.headers.RawHeader("Retry-After", "5")
                      ) {
                        complete(StatusCodes.Conflict,
                          JsObject("job_id" -> JsString(jobId),
                                   "stage" -> JsString(s.stage),
                                   "message" -> JsString("aún en proceso")))
                      }
                    case None =>
                      complete(StatusCodes.NotFound,
                        JsObject("error" -> JsString("not_found"), "job_id" -> JsString(jobId)))
                  }
                }
              }
            )
          },
          path("health") {
            get { complete(StatusCodes.OK, JsObject("status" -> JsString("ok"))) }
          },
          path("ready") {
            get {
              onComplete(registry.ask[List[JobStatus]](RegistryCommand.ListJobs)) {
                case Success(_) => complete(StatusCodes.OK, JsObject("status" -> JsString("ready")))
                case Failure(_) => complete(StatusCodes.ServiceUnavailable,
                                     JsObject("status" -> JsString("not_ready")))
              }
            }
          }
        )
      }
    }
}
