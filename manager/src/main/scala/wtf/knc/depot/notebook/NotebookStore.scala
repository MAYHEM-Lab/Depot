package wtf.knc.depot.notebook

import com.fasterxml.jackson.databind.JsonNode
import com.twitter.util.{Await, Future, FuturePool}
import com.twitter.util.jackson.ScalaObjectMapper
import javax.inject.{Inject, Singleton}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import wtf.knc.depot.notebook.NotebookStore.NotebookContents

object NotebookStore {
  type NotebookContents = JsonNode
  final val EmptyNotebook = Map(
    "cells" -> Seq.empty,
    "metadata" -> Map.empty,
    "nbformat" -> 4,
    "nbformat_minor" -> 4
  )
}

trait NotebookStore {
  def get(tag: String): Future[NotebookContents]
  def save(tag: String, contents: NotebookContents): Future[Unit]
}

@Singleton
class CloudNotebookStore @Inject() (
  objectMapper: ScalaObjectMapper,
  s3: RestS3Service,
  s3Pool: FuturePool
) extends NotebookStore {
  private final val NotebookDir = "depot.notebooks"
  Await.result {
    s3Pool { s3.createBucket(NotebookDir) }
  }

  override def get(tag: String): Future[NotebookContents] = s3Pool {
    val data = s3.getObject(NotebookDir, tag).getDataInputStream
    objectMapper.parse[NotebookContents](data)
  }

  override def save(tag: String, contents: NotebookContents): Future[Unit] = s3Pool {
    val s3Object = new S3Object(tag, objectMapper.writeValueAsBytes(contents))
    s3.putObject(NotebookDir, s3Object)
  }
}
