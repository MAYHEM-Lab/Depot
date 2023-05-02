package wtf.knc.depot.controller

import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.{QueryParam, RouteParam}
import com.twitter.inject.Logging
import com.twitter.util.Future
import javax.inject.{Inject, Provider, Singleton}
import wtf.knc.depot.controller.EntityController._
import wtf.knc.depot.dao.{ClusterDAO, EntityDAO}
import wtf.knc.depot.model.{Entity, Role}
import wtf.knc.depot.service.{CloudService, QuotaAllocation, QuotaService, QuotaUsage}

object EntityController {
  case class ListRequest(
    @QueryParam searchUser: Option[String],
    @QueryParam searchAll: Option[String],
    @QueryParam authorized: Option[Boolean],
    @QueryParam id: Option[Long]
  )

  case class QuotaResponse(allocation: QuotaAllocation, usage: QuotaUsage)

  case class OrganizationAddRequest(@RouteParam entityName: String, memberName: String, role: Role) extends EntityRoute
  case class OrganizationRemoveRequest(@RouteParam entityName: String, memberName: String) extends EntityRoute

  case class OrganizationMember(entity: Entity, role: Role)

  case class KeysResponse(accessKey: String, secretKey: String)

  private case class ManageResponse(
    owner: Boolean
  )

  case class MembersResponse(members: Seq[OrganizationMember])
  case class EntitiesResponse(entities: Seq[Entity])
}

@Singleton
class EntityController @Inject() (
  override val entityDAO: EntityDAO,
  override val clusterDAO: ClusterDAO,
  override val authProvider: Provider[Option[Auth]],
  cloudService: CloudService,
  quotaService: QuotaService
) extends Controller
  with Logging
  with EntityRequests
  with Authentication {

  private def organization(role: Option[Role])(implicit req: EntityRoute): Future[Entity] = entity(role).map {
    case org: Entity.Organization => org
    case _ => throw response.notFound.toException
  }

  prefix("/api/entity") {
    get("/?") { req: ListRequest =>
      val auth = authProvider.get
      if (req.authorized.isDefined) {
        auth match {
          case Some(Auth.User(userId)) =>
            Future
              .join(
                entityDAO.byId(userId),
                entityDAO.whereMember(userId)
              )
              .map { case (maybeUser, orgs) =>
                response.ok(EntitiesResponse(maybeUser.fold(orgs)(_ +: orgs)))
              }
          case _ => Future.value(response.forbidden)
        }

      } else if (req.id.isDefined) {
        entityDAO.byId(req.id.get).map {
          case Some(entity) => response.ok(entity)
          case _ => response.notFound
        }
      } else if (req.searchUser.isDefined) {
        entityDAO.searchUsers(req.searchUser.get).map { entities =>
          response.ok(EntitiesResponse(entities))
        }
      } else if (req.searchAll.isDefined) {
        entityDAO.searchAll(req.searchAll.get).map { entities =>
          response.ok(EntitiesResponse(entities))
        }
      } else {
        Future.value(response.ok(EntitiesResponse(Seq.empty)))
      }
    }

    prefix("/:entity_name") {
      get("/?") { implicit req: EntityRequest =>
        entity(None).map(response.ok)
      }

      post("/?") { implicit req: EntityRequest =>
        client match {
          case Some(Auth.User(userId)) =>
            entityDAO.byId(userId).flatMap {
              case Some(creator) =>
                for {
                  (accessKey, secretKey) <- cloudService.createEntity(req.entityName)
                  _ <- cloudService.addMember(creator.name, req.entityName)
                  _ <- entityDAO.createOrganization(req.entityName, userId, accessKey, secretKey)
                } yield response.ok
              case _ => Future.value(response.notFound)
            }
          case _ => response.forbidden
        }
      }

      get("/quota") { implicit req: EntityRequest =>
        entity(Some(Role.Owner)).flatMap { entity =>
          Future
            .join(
              quotaService.allocatedQuota(entity.id),
              quotaService.consumedQuota(entity.id)
            )
            .map { Function.tupled(QuotaResponse) }
            .map(response.ok)
        }
      }

      get("/manage") { implicit req: EntityRequest =>
        entity(Some(Role.Owner))
          .map { _ => true }
          .handle { _ => false }
          .map { a => response.ok(ManageResponse(a)) }
      }

      prefix("/members") {
        get("/?") { implicit req: EntityRequest =>
          entity(None).flatMap {
            case org: Entity.Organization =>
              entityDAO.members(org.id).flatMap { members =>
                val loadMembers = members.map { case (id, role) =>
                  entityDAO.byId(id).map {
                    case Some(entity) => OrganizationMember(entity, role)
                    case _ => throw new Exception(s"Unable to load entity $id")
                  }
                }
                Future.collect(loadMembers.toSeq).map { members =>
                  response.ok(members)
                }
              }
            case _ => Future.value(response.notFound)
          }
        }

        delete("/?") { implicit req: OrganizationRemoveRequest =>
          organization(Some(Role.Owner)).flatMap { org =>
            entityDAO.byName(req.memberName).flatMap {
              case Some(user: Entity.User) =>
                entityDAO.members(org.id).flatMap { members =>
                  val owners = members.filter(_._2 == Role.Owner).keySet
                  if (owners.size == 1 && owners.head == user.id) {
                    Future.value(response.badRequest("Cannot leave as last remaining owner of organization"))
                  } else {
                    cloudService.removeMember(user.name, org.name).before {
                      entityDAO.removeMember(org.id, user.id).map(_ => response.ok)
                    }
                  }
                }
              case _ => Future.value(response.notFound)
            }
          }
        }

        post("/?") { implicit req: OrganizationAddRequest =>
          organization(Some(Role.Owner)).flatMap { org =>
            entityDAO.byName(req.memberName).flatMap {
              case Some(user: Entity.User) =>
                cloudService.addMember(user.name, org.name).before {
                  entityDAO.addMember(org.id, user.id, req.role).map(_ => response.ok)
                }
              case _ => Future.value(response.notFound)
            }
          }
        }
      }
    }
  }
}
