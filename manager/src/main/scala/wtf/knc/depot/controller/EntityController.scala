package wtf.knc.depot.controller

import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.finatra.http.annotations.QueryParam
import com.twitter.inject.Logging
import com.twitter.util.Future
import javax.inject.{Inject, Provider, Singleton}
import wtf.knc.depot.controller.EntityController._
import wtf.knc.depot.dao.{ClusterDAO, EntityDAO}
import wtf.knc.depot.model.{Entity, Role}
import wtf.knc.depot.service.CloudService

object EntityController {
  case class ListRequest(
    @QueryParam searchUser: Option[String],
    @QueryParam searchAll: Option[String],
    @QueryParam authorized: Option[Boolean]
  )

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
  cloudService: CloudService
) extends Controller
  with Logging
  with Authentication {

  private def withOrganization[A <: EntityRoute](role: Option[Role])(
    fn: (A, Entity.Organization) => Future[Response]
  ): A => Future[Response] = withEntity[A](role) {
    case (req, org: Entity.Organization) => fn(req, org)
    case _ => Future.value(response.badRequest)
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
      get("/?") { req: EntityRequest =>
        entityDAO.byName(req.entityName).map {
          case Some(entity) => response.ok(entity)
          case _ => response.notFound
        }
      }

      post("/?") {
        withAuth[EntityRequest] {
          case req -> Some(Auth.User(userId)) =>
            entityDAO.byId(userId).flatMap {
              case Some(creator) =>
                for {
                  (accessKey, secretKey) <- cloudService.createEntity(req.entityName)
                  _ <- cloudService.addMember(creator.name, req.entityName)
                  _ <- entityDAO.createOrganization(req.entityName, userId, accessKey, secretKey)
                } yield response.ok
              case _ => Future.value(response.notFound)
            }
          case _ => Future.value(response.forbidden)
        }
      }

      get("/manage") { req: EntityRequest =>
        val asAdmin = withEntity[EntityRequest](Some(Role.Owner)) { (_, entity) =>
          Future.value(response.ok(entity))
        }(req)
        asAdmin.map { proxy =>
          if (proxy.status == Status.Ok) {
            response.ok(ManageResponse(true))
          } else {
            response.ok(ManageResponse(false))
          }
        }
      }

      prefix("/members") {
        get("/?") { req: EntityRequest =>
          entityDAO.byName(req.entityName).flatMap {
            case Some(org: Entity.Organization) =>
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

        delete("/?") {
          withOrganization[OrganizationRemoveRequest](Some(Role.Owner)) { (req, org) =>
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

        post("/?") {
          withOrganization[OrganizationAddRequest](Some(Role.Owner)) { (req, org) =>
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
