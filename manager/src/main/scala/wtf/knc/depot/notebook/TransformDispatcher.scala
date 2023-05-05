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

  case class AnnounceStreamingRequest(
                                       datasetId: Long,
                                       datasetTag: String,
                                       segmentId: Long,
                                       segmentVersion: Long,
                                       startOffset: Long,
                                       endOffset: Long,
                                       notebookTag: String,
                                       topic: String,
                                       bootstrapServer: String
                                     )

  case class ConsumerResponse(
                               success: String
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

   def start_stream_announce(
    clusterId: Long,
                                     dataset_id: Long,
                                     dataset_tag: String,
                                     segment_id: Long,
                                     segment_version: Long,
                                     start_offset: Long,
                                     end_offset: Long,
                                     topic: String,
                                     notebook_tag: String,
                                     bootstrap_server: String
                                   ): Future[String] = {
    clusterDAO.consumer(clusterId).flatMap {
          case (Some(consumerInfo)) =>
            val client = Http.client.newService(consumerInfo.consumer)
            val body = AnnounceStreamingRequest(dataset_id, dataset_tag, segment_id, segment_version, start_offset, end_offset, notebook_tag, topic, bootstrap_server)
            val data = objectMapper.writeValueAsBuf(body)
            val request = Request(Version.Http11, Method.Post, "/announce")
            request.content = data
            request.contentType = MediaType.Json
            client(request)
              .map { response =>
                objectMapper.parse[ConsumerResponse](response.content)
              }.map(_.success)
              .ensure(client.close())
          case _ => Future.exception(new Exception(s"Cluster ${clusterId} does not have a consumer"))
        }
      }
}
