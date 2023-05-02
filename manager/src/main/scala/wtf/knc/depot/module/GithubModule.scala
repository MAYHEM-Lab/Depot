package wtf.knc.depot.module

import com.google.inject
import com.google.inject.Provides
import com.twitter.finagle.Http
import com.twitter.finagle.http.{Fields, MediaType, Method, Request}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.httpclient.HttpClient
import com.twitter.finatra.httpclient.modules.HttpClientModuleTrait
import com.twitter.inject.annotations.Flag
import com.twitter.inject.{Injector, TwitterModule}
import com.twitter.util.Future
import com.twitter.util.jackson.ScalaObjectMapper
import javax.inject.{Inject, Named, Singleton}
import wtf.knc.depot.module.GithubClient.{AuthResponse, UserResponse}

private object GithubClient {
  case class AuthResponse(accessToken: String)
  case class UserResponse(login: String, id: Long)
}

@Singleton
class GithubClient @Inject() (
  @Flag("oauth.github.client_id") clientId: String,
  @Flag("oauth.github.client_secret") clientSecret: String,
  @Named("githubAuthClient") githubAuthClient: HttpClient,
  @Named("githubApiClient") githubApiClient: HttpClient
) {
  val authUrl: String = s"https://github.com/login/oauth/authorize?client_id=$clientId"

  def accessToken(code: String): Future[String] = {
    val req = Request(
      Method.Post,
      Request.queryString(
        "/login/oauth/access_token",
        "client_id" -> clientId,
        "client_secret" -> clientSecret,
        "code" -> code
      )
    )
    githubAuthClient
      .executeJson[AuthResponse](req)
      .map(_.accessToken)
  }

  def user(accessToken: String): Future[Long] = {
    val req = Request(Method.Get, "/user")
    req.headerMap.set("Authorization", s"token $accessToken")
    githubApiClient
      .executeJson[UserResponse](req)
      .map(_.id)
  }
}

object GithubModule extends TwitterModule {
  flag[String](
    "jwt.secret",
    "Secret for generating JWT tokens"
  )

  flag[String](
    "admin.access_key",
    "Admin API access key"
  )

  flag[String](
    "oauth.github.client_id",
    "GitHub App client ID"
  )

  flag[String](
    "oauth.github.client_secret",
    "GitHub App client secret"
  )

  override def modules: Seq[inject.Module] = Seq(GithubApiModule, GithubAuthModule)
}

private trait HttpsClientModuleTrait extends HttpClientModuleTrait {
  val serverHost: String
  override lazy val dest: String = s"$serverHost:443"

  override def defaultHeaders: Map[String, String] = Map(
    Fields.Accept -> MediaType.Json,
    Fields.UserAgent -> "depot-manager"
  )

  override def configureClient(
    injector: Injector,
    client: Http.Client
  ): Http.Client = client.withTls(serverHost)
}

private object GithubApiModule extends HttpsClientModuleTrait {
  override val serverHost = "api.github.com"
  override val label: String = "github-api"

  @Provides
  @Singleton
  @Named("githubApiClient")
  final def provideMyHttpClient(
    injector: Injector,
    statsReceiver: StatsReceiver,
    mapper: ScalaObjectMapper
  ): HttpClient = newHttpClient(injector, statsReceiver, mapper)
}

private object GithubAuthModule extends HttpsClientModuleTrait {
  override val serverHost = "github.com"
  override val label: String = "github-auth"

  @Provides
  @Singleton
  @Named("githubAuthClient")
  final def provideMyHttpClient(
    injector: Injector,
    statsReceiver: StatsReceiver,
    mapper: ScalaObjectMapper
  ): HttpClient = newHttpClient(injector, statsReceiver, mapper)
}
