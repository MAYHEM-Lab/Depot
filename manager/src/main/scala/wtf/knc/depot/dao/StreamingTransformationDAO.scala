package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.util.Future
import wtf.knc.depot.model.{Notebook, StreamingMapping}

import javax.inject.{Inject, Singleton}

trait StreamingTransformationDAO {
  def create(datasetId: Long, topic: String, notebookTag: String): Future[Unit]
  def get(datasetId: Long): Future[Option[String]]
  def getByTopic(topic: String): Future[Seq[StreamingMapping]]
}

@Singleton
class MysqlStreamingTransformationDAO @Inject() (
                                         client: Client with Transactions
                                       ) extends StreamingTransformationDAO {

  private def extract(r: Row): StreamingMapping = {
    val datasetId = r.longOrZero("dataset_id")
    val topic = r.stringOrNull("topic")
    val notebook_tag = r.stringOrNull("notebook_tag")
    StreamingMapping(datasetId, topic, notebook_tag)
  }

  final val ByDataset = "SELECT * FROM stream_dataset_mapping WHERE dataset_id = ?"
  final val ByNotebook = "SELECT * FROM stream_dataset_mapping WHERE notebook_tag = ?"
  final val CreateTransformation = "INSERT INTO stream_dataset_mapping(dataset_id, topic, notebook_tag) VALUES(?, ?, ?)"

  def create(datasetId: Long, topic: String, notebookTag: String): Future[Unit] = client.transaction { tx =>
    tx
      .prepare(CreateTransformation)
      .modify(datasetId, topic, notebookTag)
      .unit
  }

  def get(datasetId: Long): Future[Option[String]] = client
    .prepare(ByDataset)
    .select(datasetId)(_.stringOrNull("notebook_tag"))
    .map(_.headOption)

  override def getByTopic(topic: String): Future[Seq[StreamingMapping]] = client
    .prepare("SELECT * FROM stream_dataset_mapping WHERE topic = ?")
    .select(topic)(extract)
}
