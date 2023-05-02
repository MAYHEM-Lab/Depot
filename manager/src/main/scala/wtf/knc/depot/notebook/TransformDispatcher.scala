package wtf.knc.depot.notebook

import com.twitter.finagle.Http
import com.twitter.finagle.http.{MediaType, Method, Request, Version}
import com.twitter.inject.Logging
import com.twitter.util.Future
import com.twitter.util.jackson.ScalaObjectMapper
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.dao.ClusterDAO
import wtf.knc.depot.notebook.NotebookStore.NotebookContents
import wtf.knc.depot.notebook.TransformDispatcher._

object TransformDispatcher {
  case class TransformRequest(
    content: NotebookContents,
    entity: String,
    tag: String,
    version: Long,
    path: String,
    id: String
  )

  case class TransformResponse(
    artifactId: String
  )
}

@Singleton
class TransformDispatcher @Inject() (
  objectMapper: ScalaObjectMapper,
  notebookStore: NotebookStore,
  clusterDAO: ClusterDAO
) extends Logging {

  def transform(
    notebookId: String,
    clusterId: Long,
    transformationId: String,
    path: String,
    datasetEntity: String,
    datasetTag: String,
    version: Long
  ): Future[String] = {
    Future
      .join(
        notebookStore.get(notebookId),
        clusterDAO.transformer(clusterId)
      )
      .flatMap {
        case (content, Some(transformerInfo)) =>
          val client = Http.client.newService(transformerInfo.transformer)
          val body = TransformRequest(content, datasetEntity, datasetTag, version, path, transformationId)
          val data = objectMapper.writeValueAsBuf(body)
          val request = Request(Version.Http11, Method.Post, "/transform")
          request.content = data
          request.contentType = MediaType.Json
          client(request)
            .map { response =>
              objectMapper.parse[TransformResponse](response.content)
            }
            .map { _.artifactId }
            .ensure(client.close())
        case _ => Future.exception(new Exception(s"Cluster $clusterId does not have a transformer"))
      }
  }
}
