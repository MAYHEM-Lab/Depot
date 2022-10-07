package wtf.knc.depot.controller

import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.util.Future
import javax.inject.Provider
import wtf.knc.depot.dao.{ClusterDAO, EntityDAO}
import wtf.knc.depot.model.{Entity, Role}

object EntityRequests {
  trait EntityRoute { val entityName: String }
  case class EntityRequest(@RouteParam entityName: String) extends EntityRoute
}

trait EntityRequests { self: Controller with Authentication =>
  val entityDAO: EntityDAO
  val clusterDAO: ClusterDAO
  val authProvider: Provider[Option[Auth]]

  def entity(role: Option[Role])(implicit req: EntityRoute): Future[Entity] =
    entityDAO.byName(req.entityName).flatMap {
      case Some(entity) =>
        (client, role) match {
          case (_, None) => Future.value(entity)
          case (None, Some(_)) => throw response.unauthorized.toException
          case (Some(Auth.Admin), _) => Future.value(entity)

          case (Some(Auth.Cluster(clusterId)), Some(role)) =>
            clusterDAO
              .byId(clusterId)
              .flatMap { cluster => authorizeEntity(cluster.ownerId, role, entity) }
              .map {
                if (_) entity
                else throw response.forbidden.toException
              }

          case (Some(Auth.User(userId)), Some(role)) =>
            authorizeEntity(userId, role, entity).map {
              if (_) entity
              else throw response.forbidden.toException
            }
        }
      case _ => throw response.notFound.toException
    }

}
