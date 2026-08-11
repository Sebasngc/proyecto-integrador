package com.bigdata.orchestrator.util

import akka.actor.typed.ActorSystem

import java.io.IOException
import java.net.{ConnectException, SocketTimeoutException, UnknownHostException}
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/** =====================================================================
  * Reintentos con backoff exponencial y jitter.
  *
  * Tres reglas que se aplican aquí y que se justifican en el informe:
  *
  * 1. **Sólo se reintenta lo reintentable.** Un 400 (payload mal formado) o un
  *    403 no mejora por repetirlo: gasta cuota y retrasa el fallo que el
  *    cliente necesita ver. Se reintentan timeouts, errores de conexión y 5xx.
  *
  * 2. **Backoff exponencial**: 300 ms, 600 ms, 1.2 s, 2.4 s... Un reintento
  *    inmediato sobre un servicio saturado lo satura más.
  *
  * 3. **Jitter (±25%)**: sin él, N clientes que fallan a la vez reintentan a la
  *    vez y sincronizan sus picos indefinidamente. Con jitter se dispersan.
  *
  * `akka.pattern.after` programa la espera en el scheduler del sistema: NO
  * bloquea ningún hilo (nada de Thread.sleep dentro de un actor).
  * ===================================================================== */
object RetrySupport {

  /** Excepción envolvente para poder marcar un fallo como definitivo. */
  final case class NonRetryable(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)

  def isRetryable(ex: Throwable): Boolean = ex match {
    case _: NonRetryable          => false
    case _: TimeoutException      => true
    case _: SocketTimeoutException=> true
    case _: ConnectException      => true
    case _: UnknownHostException  => false  // DNS mal configurado no se arregla solo
    case _: IOException           => true
    case e: RuntimeException      =>
      val m = Option(e.getMessage).getOrElse("")
      // 5xx y 429 sí; 4xx (salvo 429) no.
      m.contains("HTTP 5") || m.contains("HTTP 429") ||
        m.contains("timeout") || m.contains("Connection refused")
    case _ => false
  }

  def retry[T](
      op: () => Future[T],
      maxAttempts: Int,
      base: FiniteDuration,
      system: ActorSystem[_],
      maxDelay: FiniteDuration = 30.seconds
  ): Future[T] = {
    implicit val ec: ExecutionContext = system.executionContext
    val classic = system.classicSystem

    def attempt(n: Int): Future[T] =
      op().recoverWith {
        case ex if n < maxAttempts && isRetryable(ex) =>
          val exp: FiniteDuration = base * math.pow(2, (n - 1).toDouble).toLong
          val jittered: FiniteDuration = (exp.toMillis * (0.75 + Random.nextDouble() * 0.5)).toLong.millis
          val delay: FiniteDuration = if (jittered > maxDelay) maxDelay else jittered
          system.log.warn("intento {}/{} falló ({}); reintentando en {}",
                          n, maxAttempts, ex.getMessage, delay)
          akka.pattern.after(delay)(attempt(n + 1))(classic)
      }

    attempt(1)
  }
}

/** Helper JSON minimalista para no arrastrar una segunda librería de JSON en el
  * lado cliente (spray-json ya se usa en las rutas; aquí sólo hacen falta
  * lecturas puntuales de respuestas de servicios externos). */
object Json {
  import spray.json._

  def obj(fields: (String, Any)*): String =
    JsObject(fields.map { case (k, v) => k -> toJs(v) }.toMap).compactPrint

  private def toJs(v: Any): JsValue = v match {
    case s: String  => JsString(s)
    case i: Int     => JsNumber(i)
    case l: Long    => JsNumber(l)
    case d: Double  => JsNumber(d)
    case b: Boolean => JsBoolean(b)
    case null       => JsNull
    case other      => JsString(other.toString)
  }

  final case class Node(value: JsValue) {
    private def field(name: String): Option[JsValue] = value match {
      case JsObject(fields) => fields.get(name)
      case _                => None
    }
    def str(name: String): Option[String] = field(name).collect {
      case JsString(s) => s
      case JsNumber(n) => n.toString
    }
    def double(name: String): Option[Double] = field(name).collect { case JsNumber(n) => n.toDouble }
    def long(name: String): Option[Long]     = field(name).collect { case JsNumber(n) => n.toLong }
    def bool(name: String): Option[Boolean]  = field(name).collect { case JsBoolean(b) => b }
    def nested(name: String): Option[Node]   = field(name).map(Node)
  }

  def parse(s: String): Node = Node(s.parseJson)

  def encodeStatus(status: com.bigdata.orchestrator.protocol.JobStatus): String = {
    import com.bigdata.orchestrator.http.JsonFormats._
    status.toJson.compactPrint
  }
}
