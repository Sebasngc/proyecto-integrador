package com.bigdata.orchestrator.repo

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import com.bigdata.orchestrator.protocol.JobStatus

/** =====================================================================
  * Persistencia del resultado final, separada de la persistencia del *proceso*.
  *
  * Hay dos persistencias distintas en este sistema y conviene no confundirlas:
  *
  *  - **Journal de Akka Persistence** (JobOrchestrator): guarda los EVENTOS
  *    para poder recuperar un job a medias tras una caída. Es infraestructura
  *    interna; nadie lo consulta desde fuera.
  *
  *  - **JobRepository** (esto): guarda el RESULTADO final en un almacén
  *    consultable por la API, con TTL. Es lo que lee `GET /jobs/{id}` cuando
  *    el actor ya no existe.
  *
  * La interfaz devuelve Future y no bloquea: llamarla desde un actor es seguro
  * siempre que la respuesta se reincorpore como mensaje (pipeToSelf), nunca con
  * Await.
  * ===================================================================== */
trait JobRepository {
  def save(status: JobStatus): Future[Unit]
  def find(jobId: String): Future[Option[JobStatus]]
  def list(limit: Int): Future[List[JobStatus]]
}

/** Implementación en memoria: para la demo local y los tests. */
final class InMemoryJobRepository(implicit ec: ExecutionContext) extends JobRepository {
  private val store = new TrieMap[String, JobStatus]()

  def save(status: JobStatus): Future[Unit] = Future {
    store.put(status.jobId, status)
    ()
  }

  def find(jobId: String): Future[Option[JobStatus]] = Future(store.get(jobId))

  def list(limit: Int): Future[List[JobStatus]] = Future {
    store.values.toList.sortBy(_.updatedAt)(Ordering[java.time.Instant].reverse).take(limit)
  }
}

/** Implementación DynamoDB para el despliegue serverless.
  *
  * DynamoDB encaja aquí porque el patrón de acceso es exactamente el que hace
  * bien: escritura por clave (jobId) y lectura por clave. No hay consultas
  * analíticas sobre esta tabla. Con facturación bajo demanda no hay que
  * aprovisionar capacidad, coherente con el resto de la arquitectura.
  *
  * El campo `ttl` deja que DynamoDB borre solo los jobs viejos: sin cron, sin
  * job de limpieza, sin coste.
  */
final class DynamoJobRepository(tableName: String)(implicit ec: ExecutionContext) extends JobRepository {
  import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
  import software.amazon.awssdk.services.dynamodb.model._
  import com.bigdata.orchestrator.http.JsonFormats._
  import spray.json._

  private val client = DynamoDbAsyncClient.create()
  private val ttlDays = 30L

  def save(status: JobStatus): Future[Unit] = {
    val item = Map(
      "job_id"     -> AttributeValue.builder().s(status.jobId).build(),
      "stage"      -> AttributeValue.builder().s(status.stage).build(),
      "updated_at" -> AttributeValue.builder().s(status.updatedAt.toString).build(),
      "payload"    -> AttributeValue.builder().s(status.toJson.compactPrint).build(),
      "ttl"        -> AttributeValue.builder()
                        .n((System.currentTimeMillis() / 1000 + ttlDays * 86400).toString).build()
    ).asJava

    val req = PutItemRequest.builder().tableName(tableName).item(item).build()
    toScala(client.putItem(req)).map(_ => ())
  }

  def find(jobId: String): Future[Option[JobStatus]] = {
    val req = GetItemRequest.builder()
      .tableName(tableName)
      .key(Map("job_id" -> AttributeValue.builder().s(jobId).build()).asJava)
      .build()
    toScala(client.getItem(req)).map { res =>
      if (!res.hasItem) None
      else Option(res.item().get("payload")).map(_.s().parseJson.convertTo[JobStatus])
    }
  }

  def list(limit: Int): Future[List[JobStatus]] = {
    // Scan sólo vale para la vista de administración de la demo. En producción
    // se añade un GSI por (stage, updated_at) y se consulta con Query.
    val req = ScanRequest.builder().tableName(tableName).limit(limit).build()
    toScala(client.scan(req)).map { res =>
      res.items().asScala.toList.flatMap { item =>
        Option(item.get("payload")).map(_.s().parseJson.convertTo[JobStatus])
      }.sortBy(_.updatedAt)(Ordering[java.time.Instant].reverse)
    }
  }

  private def toScala[T](cf: java.util.concurrent.CompletableFuture[T]): Future[T] = {
    val p = scala.concurrent.Promise[T]()
    cf.whenComplete { (value, err) =>
      if (err != null) p.failure(err) else p.success(value)
    }
    p.future
  }
}
