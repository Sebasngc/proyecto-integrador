package com.bigdata.orchestrator.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}

import java.util.UUID
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import com.bigdata.orchestrator.protocol._
import com.bigdata.orchestrator.repo.JobRepository

/** =====================================================================
  * JobRegistry — supervisor de primer nivel y enrutador por jobId.
  *
  * Es el "padre" de todos los JobOrchestrator. Sus responsabilidades:
  *  - crear un actor hijo por job entrante y devolver el jobId al instante
  *    (la API es asíncrona: responde 202 Accepted, no espera al pipeline);
  *  - enrutar las consultas de estado al hijo correcto, y si el hijo ya
  *    terminó y fue liberado, servir el estado desde el repositorio;
  *  - vigilar la terminación de los hijos (`context.watchWith`) para limpiar
  *    el mapa y no filtrar memoria en un proceso de larga vida.
  *
  * En un despliegue de varios nodos, este actor se sustituye por Cluster
  * Sharding (`ClusterSharding(system).init(Entity(...))`): mismo protocolo,
  * pero la entidad "job-<id>" vive en un único nodo del clúster y se migra
  * sola si ese nodo cae. Es un cambio de ~10 líneas gracias a que el
  * orquestador ya es event-sourced.
  * ===================================================================== */
object JobRegistry {

  def apply(
      stages: JobOrchestrator.Stages,
      settings: JobOrchestrator.Settings,
      repo: JobRepository
  ): Behavior[RegistryCommand] =
    Behaviors.setup { ctx =>
      Behaviors.withStash(capacity = 1000) { _ =>
        active(ctx, stages, settings, repo, Map.empty)
      }
    }

  private def active(
      ctx: ActorContext[RegistryCommand],
      stages: JobOrchestrator.Stages,
      settings: JobOrchestrator.Settings,
      repo: JobRepository,
      children: Map[String, ActorRef[JobCommand]]
  ): Behavior[RegistryCommand] =
    Behaviors.receiveMessage {

      case RegistryCommand.SubmitJob(request, replyTo) =>
        val jobId = UUID.randomUUID().toString
        // Supervisión: si el orquestador lanza una excepción no controlada se
        // reinicia con backoff. Al ser event-sourced, al reiniciar recupera su
        // estado desde el journal y continúa donde estaba.
        val child = ctx.spawn(
          Behaviors
            .supervise(JobOrchestrator(jobId, stages, settings))
            .onFailure[Exception](
              SupervisorStrategy.restartWithBackoff(500.millis, 30.seconds, 0.2)
            ),
          s"job-$jobId"
        )
        ctx.watchWith(child, RegistryCommand.JobTerminated(jobId))
        child ! JobCommand.Start(request, replyTo)
        ctx.log.info("job {} registrado ({} jobs activos)", jobId, children.size + 1)
        active(ctx, stages, settings, repo, children + (jobId -> child))

      case RegistryCommand.QueryJob(jobId, replyTo) =>
        children.get(jobId) match {
          case Some(child) =>
            // Se reenvía el ActorRef del cliente al hijo: el hijo responde
            // directamente y el registro no se convierte en cuello de botella
            // ni tiene que mantener estado de peticiones en vuelo.
            child ! JobCommand.GetStatus(replyTo)
            Behaviors.same
          case None =>
            // El job ya terminó y su actor fue liberado: se sirve del repositorio.
            import ctx.executionContext
            repo.find(jobId).onComplete {
              case Success(opt) => replyTo ! opt
              case Failure(_)   => replyTo ! None
            }
            Behaviors.same
        }

      case RegistryCommand.ListJobs(replyTo) =>
        import ctx.executionContext
        repo.list(limit = 100).onComplete {
          case Success(list) => replyTo ! list
          case Failure(_)    => replyTo ! Nil
        }
        Behaviors.same

      case RegistryCommand.CancelJob(jobId, reason) =>
        children.get(jobId).foreach(_ ! JobCommand.Cancel(reason))
        Behaviors.same

      case RegistryCommand.JobTerminated(jobId) =>
        ctx.log.debug("actor del job {} terminado; se libera del registro", jobId)
        active(ctx, stages, settings, repo, children - jobId)
    }
}
