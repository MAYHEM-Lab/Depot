package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Transactions}
import com.twitter.util.Future
import javax.inject.{Inject, Singleton}

trait TransformationDAO {
  def create(datasetId: Long, notebookTag: String): Future[Unit]
  def get(datasetId: Long): Future[Option[String]]
}

@Singleton
class MysqlTransformationDAO @Inject() (
  client: Client with Transactions
) extends TransformationDAO {

  final val ByDataset = "SELECT * FROM transformations WHERE dataset_id = ?"
  final val CreateTransformation = "INSERT INTO transformations(dataset_id, notebook_tag) VALUES(?, ?)"

  def create(datasetId: Long, notebookTag: String): Future[Unit] = client.transaction { tx =>
    tx
      .prepare(CreateTransformation)
      .modify(datasetId, notebookTag)
      .unit
  }

  def get(datasetId: Long): Future[Option[String]] = client
    .prepare(ByDataset)
    .select(datasetId)(_.stringOrNull("notebook_tag"))
    .map(_.headOption)

}
