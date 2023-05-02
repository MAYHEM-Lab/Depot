package wtf.knc.depot.controller

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.inject.Logging
import com.twitter.util.Future
import javax.inject.{Inject, Provider, Singleton}
import wtf.knc.depot.controller.ClusterController._
import wtf.knc.depot.dao.{ClusterDAO, EntityDAO}
import wtf.knc.depot.model._

object ClusterController {
  case class ClusterRequest(
    @RouteParam entityName: String,
    @RouteParam clusterName: String
  ) extends EntityRoute

  case class ProvisionClusterRequest(
    @RouteParam entityName: String,
    @RouteParam clusterName: String,
    sparkInfo: SparkInfo,
    notebookInfo: NotebookInfo,
    transformerInfo: TransformerInfo,
    consumerInfo: ConsumerInfo
  ) extends EntityRoute

  case class ClusterKeys(accessKey: String, secretKey: String)
  case class ClusterInfo(
    cluster: Cluster,
    notebook: Option[NotebookInfo],
    spark: Option[SparkInfo],
    transformer: Option[TransformerInfo],
    consumer: Option[ConsumerInfo],
    owner: Entity,
    keys: Option[ClusterKeys]
  )
  case class ClustersResponse(clusters: Seq[ClusterInfo])
}

@Singleton
class ClusterController @Inject() (
  override val entityDAO: EntityDAO,
  override val clusterDAO: ClusterDAO,
  override val authProvider: Provider[Option[Auth]],
  authService: AuthService
) extends Controller
  with Logging
  with Authentication {

  private def withCluster(
    role: Option[Role]
  )(fn: (ClusterRequest, ClusterInfo) => Future[Response]): ClusterRequest => Future[Response] =
    withEntity[ClusterRequest](role) { (req, _) =>
      getClusterInfo(req.clusterName).flatMap {
        case Some(cluster) => fn(req, cluster)
        case _ => Future.value(response.notFound)
      }
    }

  private def getClusterInfo(name: String, withKeys: Boolean = false): Future[Option[ClusterInfo]] = {
    clusterDAO.byTag(name).flatMap {
      case Some(cluster) =>
        val loadKeys = if (withKeys) {
          entityDAO.keys(cluster.ownerId).map { case (accessKey, secretKey) =>
            Some(ClusterKeys(accessKey, secretKey))
          }
        } else {
          Future.value(None)
        }
        Future
          .join(
            clusterDAO.notebook(cluster.id),
            clusterDAO.spark(cluster.id),
            clusterDAO.transformer(cluster.id),
            clusterDAO.consumer(cluster.id),
            entityDAO.byId(cluster.ownerId),
            loadKeys
          )
          .map {
            case (notebook, spark, transformer, consumer, Some(owner), keys) =>
              Some(ClusterInfo(cluster, notebook, spark, transformer, consumer, owner, keys))
            case _ => None
          }
      case _ => Future.value(None)
    }
  }

  prefix("/api/clusters") {
    get("/?") {
      withAuth[Request] {
        case _ -> Some(Auth.User(userId)) =>
          Future
            .join(
              entityDAO.byId(userId),
              entityDAO.whereMember(userId)
            )
            .map { case (maybeUser, orgs) => maybeUser.fold(orgs)(_ +: orgs) }
            .flatMap { entities => Future.collect(entities.map(entity => clusterDAO.byOwner(entity.id))) }
            .flatMap { clusters => Future.collect(clusters.flatten.map(cluster => getClusterInfo(cluster.tag))) }
            .map { clusters => response.ok(ClustersResponse(clusters.flatten)) }

        case _ -> Some(Auth.Cluster(clusterId)) =>
          clusterDAO.byId(clusterId).flatMap { cluster =>
            getClusterInfo(cluster.tag, withKeys = true).map(response.ok)
          }
        case _ => Future.value(response.ok(ClustersResponse(Seq.empty)))
      }
    }

    prefix("/:entity_name") {
      get("/?") {
        withEntity[EntityRequest](Some(Role.Member)) { (_, entity) =>
          clusterDAO
            .byOwner(entity.id)
            .flatMap { clusters => Future.collect(clusters.map(cluster => getClusterInfo(cluster.tag))) }
            .map { clusters =>
              response.ok(ClustersResponse(clusters.flatten))
            }
        }
      }

      prefix("/:cluster_name") {
        post("/?") {
          withAuth[ClusterRequest] {
            case (req, Some(Auth.Admin)) =>
              val loadCluster = clusterDAO.byTag(req.clusterName)
              val loadEntity = entityDAO.byName(req.entityName)
              Future.join(loadCluster, loadEntity).flatMap {
                case (Some(_), _) =>
                  Future.value(response.badRequest(s"Cluster ${req.clusterName} already exists"))
                case (_, Some(entity)) =>
                  clusterDAO
                    .create(entity.id, req.clusterName)
                    .flatMap(clusterDAO.byId)
                    .map(response.created)
                case _ => Future.value(response.notFound(s"Could not find entity ${req.entityName}"))
              }
            case _ => Future.value(response.forbidden)
          }
        }

        patch("/?") {
          withAuth[ProvisionClusterRequest] {
            case (req, Some(Auth.Admin)) =>
              clusterDAO.byTag(req.clusterName).flatMap {
                case Some(cluster) =>
                  clusterDAO
                    .provision(cluster.id, req.sparkInfo, req.notebookInfo, req.transformerInfo, req.consumerInfo)
                    .map(_ => response.noContent)
                case _ => Future.value(response.notFound)
              }
            case _ => Future.value(response.forbidden)
          }
        }

        delete("/?") {
          withAuth[ClusterRequest] {
            case (req, Some(Auth.Admin)) =>
              clusterDAO.byTag(req.clusterName).flatMap {
                case Some(cluster) =>
                  clusterDAO
                    .delete(cluster.id)
                    .map(_ => response.noContent)
                case _ => Future.value(response.notFound)
              }
            case _ => Future.value(response.forbidden)
          }
        }

        get("/?") {
          withCluster(Some(Role.Member)) { (_, cluster) =>
            Future.value(response.ok(cluster))
          }
        }

        get("/token") {
          withAuth[ClusterRequest] {
            case (req, Some(Auth.Admin)) =>
              clusterDAO.byTag(req.clusterName).map {
                case Some(cluster) => response.ok(authService.clusterAccessKey(cluster.id))
                case _ => response.notFound
              }
            case _ => Future.value(response.forbidden)
          }
        }
      }
    }
  }
}
