package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.util.{Duration, Future}
import com.twitter.util.jackson.ScalaObjectMapper
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.model._
import com.twitter.conversions.DurationOps._

trait DatasetDAO {
  def byId(datasetId: Long): Future[Dataset]
  def byTag(tag: String): Future[Option[Dataset]]
  def byOwner(ownerId: Long): Future[Seq[Dataset]]
  def create(
    tag: String,
    description: String,
    owner: Long,
    datatype: Datatype,
    visibility: Visibility,
    retention: Option[Duration],
    schedule: Option[Duration],
    clusterAffinity: Option[Long]
  ): Future[Long]

  def addCollaborator(datasetId: Long, userId: Long, role: Role): Future[Unit]
  def removeCollaborator(datasetId: Long, userId: Long): Future[Unit]
  def collaborators(datasetId: Long): Future[Map[Long, Role]]
}

@Singleton
class MysqlDatasetDAO @Inject() (
  client: Client with Transactions,
  objectMapper: ScalaObjectMapper
) extends DatasetDAO {

  private def extract(r: Row): Dataset = Dataset(
    r.longOrZero("id"),
    r.longOrZero("owner_id"),
    r.stringOrNull("tag"),
    r.stringOrNull("description"),
    objectMapper.parse[Datatype](r.stringOrNull("datatype")),
    Visibility.parse(r.stringOrNull("visibility")),
    r.getLong("retention_ms").map(_.toLong.millis),
    r.getLong("schedule_ms").map(_.toLong.millis),
    r.getLong("preferred_cluster").map(_.toLong),
    r.longOrZero("created_at"),
    r.longOrZero("updated_at")
  )

  override def addCollaborator(datasetId: Long, userId: Long, role: Role): Future[Unit] = client
    .prepare("REPLACE INTO dataset_acl(dataset_id, entity_id, role) VALUES(?, ?, ?)")
    .modify(datasetId, userId, role.name)
    .unit

  override def removeCollaborator(datasetId: Long, userId: Long): Future[Unit] = client
    .prepare("DELETE FROM dataset_acl WHERE dataset_id = ? and entity_id = ?")
    .modify(datasetId, userId)
    .unit

  override def collaborators(datasetId: Long): Future[Map[Long, Role]] = client
    .prepare("SELECT * FROM dataset_acl WHERE dataset_id = ? ORDER BY role ASC")
    .select(datasetId)(r => r.longOrZero("entity_id") -> Role.parse(r.stringOrNull("role")))
    .map(_.toMap)

  override def byTag(tag: String): Future[Option[Dataset]] = client
    .prepare("SELECT * FROM datasets WHERE tag = ?")
    .select(tag)(extract)
    .map(_.headOption)

  override def byId(datasetId: Long): Future[Dataset] = client
    .prepare("SELECT * FROM datasets WHERE id = ?")
    .select(datasetId)(extract)
    .map(_.head)

  override def byOwner(ownerId: Long): Future[Seq[Dataset]] = client
    .prepare("SELECT * FROM datasets WHERE owner_id = ?")
    .select(ownerId)(extract)

  override def create(
    tag: String,
    description: String,
    owner: Long,
    datatype: Datatype,
    visibility: Visibility,
    retention: Option[Duration],
    schedule: Option[Duration],
    clusterAffinity: Option[Long]
  ): Future[Long] =
    client.transaction { tx =>
      val now = System.currentTimeMillis
      for {
        _ <- tx
          .prepare(
            "INSERT INTO datasets(tag, description, owner_id, datatype, visibility, retention_ms, schedule_ms, preferred_cluster, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
          )
          .modify(
            tag,
            description,
            owner,
            objectMapper.writeValueAsString(datatype),
            visibility.name,
            retention.map(_.inMillis),
            schedule.map(_.inMillis),
            clusterAffinity,
            now,
            now
          )
        id <- tx.select("SELECT LAST_INSERT_ID() as id")(_.longOrZero("id")).map(_.head)
      } yield id
    }
}