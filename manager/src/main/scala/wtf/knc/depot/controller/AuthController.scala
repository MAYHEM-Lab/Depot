package wtf.knc.depot.controller

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import com.twitter.util.Future
import javax.inject.{Inject, Provider, Singleton}
import wtf.knc.depot.controller.AuthController.{GithubAuthRequest, GithubAuthResponse, GithubRegisterRequest, UserAuthResponse}
import wtf.knc.depot.dao.{ClusterDAO, EntityDAO}
import wtf.knc.depot.model.Entity
import wtf.knc.depot.module.GithubClient
import wtf.knc.depot.service.CloudService

object AuthController {
  case class GithubAuthRequest(code: String)
  case class GithubAuthResponse(userId: Option[Long], token: String)
  case class GithubRegisterRequest(token: String, username: String)

  case class UserAuthResponse(user: Option[Entity])
}

@Singleton
class AuthController @Inject() (
  githubClient: GithubClient,
  override val entityDAO: EntityDAO,
  override val clusterDAO: ClusterDAO,
  override val authProvider: Provider[Option[Auth]],
  authService: AuthService,
  cloudService: CloudService
) extends Controller
  with Authentication {

  prefix("/api/auth") {

    get("/?") {
      withAuth[Request] {
        case _ -> Some(Auth.User(userId)) =>
          entityDAO.byId(userId).map {
            case Some(user) => response.ok(UserAuthResponse(Some(user)))
            case _ => response.ok(UserAuthResponse(None)).cookie(authService.noauthCookie)
          }
        case _ => Future.value(response.ok(UserAuthResponse(None)).cookie(authService.noauthCookie))
      }
    }

    post("/logout") { _: Request =>
      response.ok.cookie(authService.noauthCookie)
    }

    prefix("/github") {
      get("/?") { _: Request =>
        response.ok(githubClient.authUrl)
      }

      post("/?") { req: GithubAuthRequest =>
        logger.info(s"Received request to authenticate: ${req.code}")
        githubClient
          .accessToken(req.code)
          .flatMap { token =>
            logger.info(s"Got auth token: $token")
            githubClient
              .user(token)
              .flatMap { githubUserId =>
                logger.info(s"Found GitHub user: $githubUserId")
                entityDAO.byGithubId(githubUserId).map {
                  case Some(userId) =>
                    logger.info(s"Corresponds to user: $userId")
                    response
                      .ok(GithubAuthResponse(Some(userId), token))
                      .cookie(authService.authCookie(userId))
                  case _ =>
                    response
                      .ok(GithubAuthResponse(None, token))
                      .cookie(authService.noauthCookie)
                }
              }
          }
      }

      post("/register") { req: GithubRegisterRequest =>
        githubClient
          .user(req.token)
          .flatMap { githubUserId =>
            cloudService.createEntity(req.username).flatMap { case (accessKey, secretKey) =>
              entityDAO.createWithGithub(req.username, githubUserId, accessKey, secretKey)
            }
          }
          .flatMap(entityDAO.byId)
          .map {
            case Some(user) =>
              response
                .ok(UserAuthResponse(Some(user)))
                .cookie(authService.authCookie(user.id))
            case _ =>
              response
                .internalServerError(UserAuthResponse(None))
                .cookie(authService.noauthCookie)
          }
      }
    }
  }
}
