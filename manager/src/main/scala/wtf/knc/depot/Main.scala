package wtf.knc.depot

import com.google.inject.Module
import com.twitter.finagle.http.filter.Cors
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.inject.requestscope.{FinagleRequestScopeFilter, FinagleRequestScopeModule}
import wtf.knc.depot.controller._
import wtf.knc.depot.module._
import wtf.knc.depot.service.MessageService

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
    .filter[FinagleRequestScopeFilter[Request, Response]]
    .filter[AuthFilter]
    .add[ClusterController]
    .add[DatasetController]
    .add[NotebookController]
    .add[EntityController]
    .add[AuthController]
    .add[ValidateController]

  override def setup(): Unit = {
    val messenger = injector.instance[MessageService]
    closeOnExit(messenger)
  }
}