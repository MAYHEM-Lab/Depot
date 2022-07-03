package wtf.knc.depot.controller

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
  with EntityRequests
  with Logging {

  def notebook(role: Option[Role])(implicit req: NotebookRoute): Future[Notebook] = entity(role)
    .flatMap { _ =>
      notebookDAO.byTag(req.notebookTag).map {
        case Some(notebook) => notebook
        case _ => throw response.notFound.toException
      }
    }

  prefix("/api/entity/:entity_name/notebooks") {
    get("/?") { implicit req: EntityRequest =>
      entity(Some(Role.Member)).flatMap { owner =>
        notebookDAO.byOwner(owner.id).map(response.ok)
      }
    }

    prefix("/:notebook_tag") {
      get("/?") { implicit req: NotebookRequest =>
        notebook(Some(Role.Member))
      }

      post("/?") { implicit req: NotebookRequest =>
        entity(Some(Role.Owner)).flatMap { owner =>
          notebookDAO.create(req.notebookTag, owner.id).flatMap { _ =>
            notebookStore
              .save(req.notebookTag, objectMapper.convert[NotebookContents](NotebookStore.EmptyNotebook))
              .map { _ => response.created }
          }
        }
      }

      prefix("/contents") {
        get("/?") { implicit req: NotebookRequest =>
          notebook(Some(Role.Member)).flatMap { notebook =>
            notebookStore
              .get(notebook.tag)
              .map(response.ok)
          }
        }

        post("/?") { implicit req: NotebookContentRequest =>
          notebook(Some(Role.Owner)).flatMap { notebook =>
            notebookStore
              .save(notebook.tag, req.content)
              .map { _ => response.created }
          }
        }
      }
    }
  }
}
