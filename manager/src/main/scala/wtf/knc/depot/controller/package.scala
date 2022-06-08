package wtf.knc.depot

import com.twitter.finagle
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.http.cookie.SameSite
import com.twitter.finagle.http.{Cookie, Request, Response}
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.inject.annotations.Flag
import com.twitter.inject.requestscope.FinagleRequestScope
import com.twitter.util.jackson.ScalaObjectMapper
import com.twitter.util.{Duration, Future}
import javax.inject.{Inject, Provider, Singleton}
import pdi.jwt.{Jwt, JwtAlgorithm}
import wtf.knc.depot.dao.{ClusterDAO, EntityDAO}
import wtf.knc.depot.model.{Entity, Role}

package object controller {
  trait EntityRoute { val entityName: String }
  case class EntityRequest(@RouteParam entityName: String) extends EntityRoute

  trait Authentication extends Controller {
    val entityDAO: EntityDAO
    val clusterDAO: ClusterDAO
    val authProvider: Provider[Option[Auth]]

    def isMember(orgId: Long, entityId: Long, role: Role): Future[Boolean] =
      entityDAO.members(orgId).map { members =>
        val authorized = if (role == Role.Member) {
          members.contains(entityId)
        } else {
          members.get(entityId).contains(role)
        }
        authorized
      }

    // Does entity `entityId` have role `role` against entity `entity`?
    def authorizeEntity(entityId: Long, role: Role, entity: Entity): Future[Boolean] = entity match {
      case user: Entity.User =>
        Future.value(user.id == entityId)
      case org: Entity.Organization =>
        if (org.id == entityId) {
          Future.value(true)
        } else {
          isMember(org.id, entityId, role)
        }
    }

    def withAuth[A](
      fn: (A, Option[Auth]) => Future[Response]
    ): A => Future[Response] = (req: A) => {
      val auth = authProvider.get
      fn(req, auth)
    }

    def withEntity[A <: EntityRoute](role: Option[Role])(
      fn: (A, Entity) => Future[Response]
    ): A => Future[Response] = {
      withAuth[A] { (req, auth) =>
        {
          entityDAO.byName(req.entityName).flatMap {
            case Some(entity) =>
              (auth, role) match {
                case (_, None) => fn(req, entity)
                case (None, Some(_)) => Future.value(response.unauthorized)
                case (Some(Auth.Admin), _) => fn(req, entity)

                case (Some(Auth.Cluster(clusterId)), Some(role)) =>
                  clusterDAO
                    .byId(clusterId)
                    .flatMap { cluster => authorizeEntity(cluster.ownerId, role, entity) }
                    .flatMap {
                      if (_) fn(req, entity)
                      else Future.value(response.forbidden)
                    }

                case (Some(Auth.User(userId)), Some(role)) =>
                  authorizeEntity(userId, role, entity).flatMap {
                    if (_) fn(req, entity)
                    else Future.value(response.forbidden)
                  }
              }
            case _ => Future.value(response.notFound)
          }
        }
      }
    }
  }

  sealed trait Auth
  object Auth {
    case class User(userId: Long) extends Auth
    case class Cluster(clusterId: Long) extends Auth
    case object Admin extends Auth
  }

  @Singleton
  class AuthService @Inject() (
    @Flag("jwt.secret") jwtSecret: String,
    @Flag("admin.access_key") adminKey: String,
    objectMapper: ScalaObjectMapper
  ) {
    private def generate(key: String, userId: Long): String = Jwt.encode(
      key + ":" + objectMapper.writeValueAsString(userId),
      jwtSecret,
      JwtAlgorithm.HS256
    )

    private def tokenCookie(token: Option[String]): Cookie = new Cookie(
      "access_token",
      token.getOrElse(""),
      path = Some("/"),
      maxAge = Some(Duration.Top),
      httpOnly = true
    )

    val noauthCookie: Cookie = tokenCookie(None)
    def authCookie(userId: Long): Cookie = tokenCookie(Some(generate("user", userId)))

    def clusterAccessKey(clusterId: Long): String = generate("cluster", clusterId)

    def isAdmin(token: String): Boolean = token == adminKey

    def read(key: String)(jwt: String): Option[Long] = Jwt
      .decodeRaw(jwt, jwtSecret, Seq(JwtAlgorithm.HS256))
      .map(_.split(":"))
      .collect {
        case Array(readKey, data) if readKey == key => objectMapper.parse[Long](data)
      }
      .toOption
  }

  @Singleton
  class AuthFilter @Inject() (requestScope: FinagleRequestScope, authService: AuthService)
    extends SimpleFilter[Request, Response] {
    override def apply(req: Request, svc: finagle.Service[Request, Response]): Future[Response] = {

      val maybeAdmin = req.headerMap
        .get("access_key")
        .filter(_.nonEmpty)
        .collect { case token if authService.isAdmin(token) => Auth.Admin }

      val maybeCluster = req.headerMap
        .get("access_key")
        .filter(_.nonEmpty)
        .flatMap(authService.read("cluster"))
        .map(Auth.Cluster)

      val maybeUser = req.cookies
        .get("access_token")
        .map(_.value)
        .filter(_.nonEmpty)
        .flatMap(authService.read("user"))
        .map(Auth.User)

      requestScope.seed[Option[Auth]](maybeAdmin.orElse(maybeCluster.orElse(maybeUser)))
      svc(req)
    }
  }
}
