package wtf.knc.depot.controller

import com.twitter.finagle.http.Response
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.inject.Logging
import com.twitter.util.Future
import com.twitter.util.jackson.ScalaObjectMapper
import javax.inject.{Inject, Provider, Singleton}
import wtf.knc.depot.controller.NotebookController.{NotebookContentRequest, NotebookRequest, NotebookRoute}
import wtf.knc.depot.dao.{ClusterDAO, EntityDAO, NotebookDAO}
import wtf.knc.depot.model.{Notebook, Role}
import wtf.knc.depot.notebook.NotebookStore
import wtf.knc.depot.notebook.NotebookStore.NotebookContents

object NotebookController {
  trait NotebookRoute extends EntityRoute { val notebookTag: String }

  private case class NotebookRequest(
    @RouteParam entityName: String,
    @RouteParam notebookTag: String
  ) extends NotebookRoute

  case class NotebookContentRequest(
    @RouteParam entityName: String,
    @RouteParam notebookTag: String,
    content: NotebookContents
  ) extends NotebookRoute
}

@Singleton
class NotebookController @Inject() (
  override val entityDAO: EntityDAO,
  override val clusterDAO: ClusterDAO,
  override val authProvider: Provider[Option[Auth]],
  objectMapper: ScalaObjectMapper,
  notebookDAO: NotebookDAO,
  notebookStore: NotebookStore
) extends Controller
  with Authentication
  with Logging {

  // delegate dataset acl checks to entity
  private def withNotebook[A <: NotebookRoute](
    role: Option[Role]
  )(fn: (A, Notebook) => Future[Response]): A => Future[Response] = withEntity[A](role) { (req, _) =>
    notebookDAO.byTag(req.notebookTag).flatMap {
      case Some(notebook) => fn(req, notebook)
      case _ => Future.value(response.notFound)
    }
  }

  prefix("/api/entity/:entity_name/notebooks") {
    get("/?") {
      withEntity[EntityRequest](Some(Role.Member)) { (_, owner) =>
        notebookDAO.byOwner(owner.id).map(response.ok)
      }
    }

    prefix("/:notebook_tag") {
      get("/?") {
        withEntity[NotebookRequest](Some(Role.Member)) { (req, _) =>
          notebookDAO.byTag(req.notebookTag).map(response.ok)
        }
      }

      post("/?") {
        withEntity[NotebookRequest](Some(Role.Member)) { (req, entity) =>
          notebookDAO.create(req.notebookTag, entity.id).flatMap { _ =>
            notebookStore
              .save(req.notebookTag, objectMapper.convert[NotebookContents](NotebookStore.EmptyNotebook))
              .map { _ => response.created }
          }
        }
      }

      prefix("/contents") {
        get("/?") {
          withNotebook[NotebookRequest](Some(Role.Member)) { (_, notebook) =>
            notebookStore
              .get(notebook.tag)
              .map(response.ok)
          }
        }

        post("/?") {
          withNotebook[NotebookContentRequest](Some(Role.Member)) { (req, notebook) =>
            notebookStore
              .save(notebook.tag, req.content)
              .map { _ => response.created }
          }
        }
      }
    }
  }
}
