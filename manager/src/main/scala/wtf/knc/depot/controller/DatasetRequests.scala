package wtf.knc.depot.controller

import com.twitter.finatra.http.Controller
import com.twitter.util.Future
import wtf.knc.depot.controller.DatasetController.{DatasetRoute, SegmentRoute}
import wtf.knc.depot.dao.{DatasetDAO, EntityDAO, SegmentDAO}
import wtf.knc.depot.model.{Dataset, Entity, Role, Segment, Visibility}

trait DatasetRequests { self: Controller with Authentication =>
  val entityDAO: EntityDAO
  val datasetDAO: DatasetDAO
  val segmentDAO: SegmentDAO

  def dataset(role: Option[Role])(implicit req: DatasetRoute): Future[Dataset] = entityDAO
    .byName(req.entityName)
    .flatMap {
      case Some(owner) =>
        datasetDAO.byTag(req.datasetTag).flatMap {
          case Some(dataset) =>
            lazy val deny = response.forbidden.toFutureException
            lazy val grant = Future.value(dataset)
            if (client.contains(Auth.Admin)) {
              grant
            } else {
              client
                .fold[Future[Option[Entity]]](Future.None) {
                  case Auth.Cluster(clusterId) =>
                    clusterDAO.byId(clusterId).flatMap { cluster =>
                      entityDAO.byId(cluster.ownerId)
                    }
                  case Auth.User(userId) =>
                    entityDAO.byId(userId)
                  case _ => Future.None
                }
                .flatMap { maybeEntity => authorizeDataset(maybeEntity, role, owner, dataset) }
                .flatMap { if (_) grant else deny }
            }
          case _ => throw response.notFound.toException
        }
      case _ => throw response.notFound.toException
    }

  def segment(role: Option[Role])(implicit req: SegmentRoute): Future[Segment] = dataset(role)
    .flatMap { dataset =>
      segmentDAO.byVersion(dataset.id, req.version).map {
        case Some(segment) => segment
        case _ => throw response.notFound.toException
      }
    }

  // Does entity `entityId` have role `role` against dataset `dataset`?
  def authorizeDataset(
    requester: Option[Entity],
    role: Option[Role],
    owner: Entity,
    dataset: Dataset
  ): Future[Boolean] = {
    (requester, role, dataset.visibility) match {
      case (_, None, _) => Future.True
      case (_, Some(Role.Member), Visibility.Public) => Future.True
      case (Some(entity), _, _) if entity == owner => Future.True
      case (Some(entity), Some(role), _) =>
        val asOrgMember = owner match {
          case Entity.Organization(id, _, _) => isMember(id, entity.id, role)
          case _ => Future.False
        }
        val asCollaborator = datasetDAO.collaborators(dataset.id).flatMap { collaborators =>
          entityDAO
            .whereMember(entity.id)
            .map(_ :+ entity)
            .map { targets =>
              targets.exists { e =>
                if (role == Role.Member) {
                  collaborators.contains(e.id)
                } else {
                  collaborators.get(e.id).contains(role)
                }
              }
            }
        }
        Future
          .join(asOrgMember, asCollaborator)
          .map(Function.tupled(_ || _))
      case _ => Future.False
    }
  }
}
