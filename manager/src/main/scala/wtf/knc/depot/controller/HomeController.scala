package wtf.knc.depot.controller

import java.util.concurrent.ConcurrentHashMap

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import com.twitter.util.Future
import javax.inject.{Inject, Provider, Singleton}
import wtf.knc.depot.controller.HomeController.HomeResponse
import wtf.knc.depot.dao._
import wtf.knc.depot.model.{Dataset, Entity, Notebook, Role}
import scala.jdk.CollectionConverters._

object HomeController {
  case class HomeResponse(datasets: Seq[Dataset], notebooks: Seq[Notebook], entities: Seq[Entity])
}

@Singleton
class HomeController @Inject() (
  override val authProvider: Provider[Option[Auth]],
  override val clusterDAO: ClusterDAO,
  override val entityDAO: EntityDAO,
  override val datasetDAO: DatasetDAO,
  override val segmentDAO: SegmentDAO
) extends Controller
  with EntityRequests
  with DatasetRequests
  with Authentication {

  prefix("/api/home") {
    get("/?") { implicit req: Request =>
      client match {
        case Some(Auth.User(userId)) =>
          entityDAO.byId(userId).flatMap { requestor =>
            datasetDAO
              .recent(50)
              .flatMap { datasets =>
                val ownerCache = new ConcurrentHashMap[Long, Future[Option[Entity]]]()
                val filterDatasets = datasets.map { dataset =>
                  ownerCache
                    .computeIfAbsent(dataset.ownerId, entityDAO.byId)
                    .flatMap {
                      case Some(owner) => authorizeDataset(requestor, Some(Role.Member), owner, dataset)
                      case _ => Future.False
                    }
                    .map { if (_) Some(dataset) else None }
                }
                Future
                  .join(
                    Future.collect(filterDatasets).map(_.flatten),
                    Future.collect(ownerCache.values().asScala.toSeq).map(_.flatten)
                  )
                  .map { case (datasets, entities) => response.ok(HomeResponse(datasets, Seq.empty, entities)) }
              }

          }
        case _ => response.badRequest("Not a user").toFutureException
      }

    }
  }
}
