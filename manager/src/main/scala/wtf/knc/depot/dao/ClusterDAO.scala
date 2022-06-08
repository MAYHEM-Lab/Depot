package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.util.Future
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.model._

trait ClusterDAO {
  def byId(clusterId: Long): Future[Cluster]
  def byTag(tag: String): Future[Option[Cluster]]
  def byOwner(ownerId: Long): Future[Seq[Cluster]]

  def create(ownerId: Long, tag: String): Future[Long]
  def delete(clusterId: Long): Future[Unit]
  def provision(
    clusterId: Long,
    sparkInfo: SparkInfo,
    notebookInfo: NotebookInfo,
    transformerInfo: TransformerInfo
  ): Future[Unit]

  def spark(clusterId: Long): Future[Option[SparkInfo]]
  def notebook(clusterId: Long): Future[Option[NotebookInfo]]
  def transformer(clusterId: Long): Future[Option[TransformerInfo]]
}

@Singleton
class MysqlClusterDAO @Inject() (
  client: Client with Transactions
) extends ClusterDAO {

  private def readCluster(r: Row): Cluster = {
    val id = r.longOrZero("id")
    val ownerId = r.longOrZero("owner_id")
    val status = ClusterStatus.parse(r.stringOrNull("status"))
    val tag = r.stringOrNull("tag")
    val createdAt = r.longOrZero("created_at")
    val updatedAt = r.longOrZero("updated_at")
    Cluster(id, ownerId, status, tag, createdAt, updatedAt)
  }

  private def readSparkInfo(r: Row): SparkInfo = {
    val sparkMaster = r.stringOrNull("spark_master")
    SparkInfo(sparkMaster)
  }

  private def readNotebookInfo(r: Row): NotebookInfo = {
    val notebookMaster = r.stringOrNull("notebook_master")
    NotebookInfo(notebookMaster)
  }

  private def readTransformerInfo(r: Row): TransformerInfo = {
    val transformer = r.stringOrNull("transformer")
    TransformerInfo(transformer)
  }

  override def create(ownerId: Long, tag: String): Future[Long] = client.transaction { tx =>
    val now = System.currentTimeMillis
    for {
      _ <- tx
        .prepare("INSERT INTO clusters(owner_id, tag, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)")
        .modify(ownerId, tag, ClusterStatus.Provisioning.name, now, now)
      id <- tx
        .select("SELECT LAST_INSERT_ID() as id")(_.longOrZero("id"))
        .map(_.head)
    } yield id
  }

  override def delete(clusterId: Long): Future[Unit] = client
    .prepare("DELETE FROM clusters WHERE id = ?")
    .modify(clusterId)
    .unit

  override def provision(
    clusterId: Long,
    sparkInfo: SparkInfo,
    notebookInfo: NotebookInfo,
    transformerInfo: TransformerInfo
  ): Future[Unit] = client.transaction { tx =>
    val spark = tx.prepare("INSERT INTO spark_info VALUES(?, ?)").modify(clusterId, sparkInfo.sparkMaster)
    val nb = tx.prepare("INSERT INTO notebook_info VALUES(?, ?)").modify(clusterId, notebookInfo.notebookMaster)
    val trans = tx.prepare("INSERT INTO transformer_info VALUES(?, ?)").modify(clusterId, transformerInfo.transformer)
    Future
      .join(spark, nb, trans)
      .flatMap { _ =>
        tx
          .prepare("UPDATE clusters SET status = ?, updated_at = ? WHERE id = ?")
          .modify(ClusterStatus.Active.name, System.currentTimeMillis, clusterId)
          .unit
      }
  }

  override def byId(clusterId: Long): Future[Cluster] = client
    .prepare("SELECT * FROM clusters WHERE id = ?")
    .select(clusterId)(readCluster)
    .map(_.head)

  override def byTag(tag: String): Future[Option[Cluster]] = client
    .prepare("SELECT * FROM clusters WHERE tag = ?")
    .select(tag)(readCluster)
    .map(_.headOption)

  override def byOwner(ownerId: Long): Future[Seq[Cluster]] = client
    .prepare("SELECT * FROM clusters WHERE owner_id = ?")
    .select(ownerId)(readCluster)

  override def spark(clusterId: Long): Future[Option[SparkInfo]] = client
    .prepare("SELECT * FROM spark_info WHERE cluster_id = ?")
    .select(clusterId)(readSparkInfo)
    .map(_.headOption)

  override def transformer(clusterId: Long): Future[Option[TransformerInfo]] = client
    .prepare("SELECT * FROM transformer_info WHERE cluster_id = ?")
    .select(clusterId)(readTransformerInfo)
    .map(_.headOption)

  override def notebook(clusterId: Long): Future[Option[NotebookInfo]] = client
    .prepare("SELECT * FROM notebook_info WHERE cluster_id = ?")
    .select(clusterId)(readNotebookInfo)
    .map(_.headOption)
}
