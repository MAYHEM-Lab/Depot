package wtf.knc.depot

import com.google.inject.Module
import com.twitter.finagle.Http
import com.twitter.finagle.http.filter.Cors
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.ExceptionMappingFilter
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.inject.requestscope.{FinagleRequestScopeFilter, FinagleRequestScopeModule}
import wtf.knc.depot.controller._
import wtf.knc.depot.module._
import wtf.knc.depot.service.MessageService
import com.twitter.conversions.StorageUnitOps._
import wtf.knc.depot.beam.streaming.WindowedWordCount

object Main extends HttpServer {

  override def modules: Seq[Module] = Seq(
    FinagleRequestScopeModule,
    AuthModule,
    GithubModule,
    CloudModule,
    MysqlModule,
    RabbitMQModule,
    NotebookModule
  )

  override def configureHttp(router: HttpRouter): Unit = router
    .filter(new Cors.HttpFilter(Cors.UnsafePermissivePolicy), beforeRouting = true)
    .filter[ExceptionMappingFilter[Request]]
    .filter[FinagleRequestScopeFilter[Request, Response]]
    .filter[AuthFilter]
    .add[HomeController]
    .add[ClusterController]
    .add[DatasetController]
    .add[NotebookController]
    .add[EntityController]
    .add[AuthController]
    .add[ValidateController]
    .add[UploadController]

  override def configureHttpServer(server: Http.Server): Http.Server = server
    .withMaxRequestSize(50.megabytes)
    .withStreaming(true)

  override def setup(): Unit = {
    val messenger = injector.instance[MessageService]
    closeOnExit(messenger)
  }
}
