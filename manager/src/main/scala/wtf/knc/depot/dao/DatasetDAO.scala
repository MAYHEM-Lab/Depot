package wtf.knc.depot.dao

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.util.jackson.ScalaObjectMapper
import com.twitter.util.{Duration, Future, Time}
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.model._

trait DatasetDAO {
  def byId(datasetId: Long): Future[Dataset]
  def byTag(tag: String): Future[Option[Dataset]]
  def byOwner(ownerId: Long): Future[Seq[Dataset]]
  def create(
    tag: String,
    description: String,
    owner: Long,
    origin: Origin,
    datatype: Datatype,
    visibility: Visibility,
    storageClass: StorageClass,
    retention: Option[Duration],
    schedule: Option[Duration],
    clusterAffinity: Option[Long]
  ): Future[Long]
  def update(
    datasetId: Long,
    description: String,
    retention: Option[Duration],
    schedule: Option[Duration],
    visibility: Visibility
  ): Future[Unit]
  def addCollaborator(datasetId: Long, userId: Long, role: Role): Future[Unit]
  def removeCollaborator(datasetId: Long, userId: Long): Future[Unit]
  def collaborators(datasetId: Long): Future[Map[Long, Role]]
  def recent(limit: Int): Future[Seq[Dataset]]
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
    Origin.parse(r.stringOrNull("origin")),
    objectMapper.parse[Datatype](r.stringOrNull("datatype")),
    Visibility.parse(r.stringOrNull("visibility")),
    StorageClass.parse(r.stringOrNull("storage_class")),
    r.getLong("retention_ms").map(_.toLong.millis),
    r.getLong("schedule_ms").map(_.toLong.millis),
    r.getLong("preferred_cluster").map(_.toLong),
    r.longOrZero("created_at"),
    r.longOrZero("updated_at")
  )

  override def recent(limit: Int): Future[Seq[Dataset]] =
    client.select(s"SELECT * FROM datasets ORDER BY created_at DESC LIMIT $limit")(extract)

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
    origin: Origin,
    datatype: Datatype,
    visibility: Visibility,
    storageClass: StorageClass,
    retention: Option[Duration],
    schedule: Option[Duration],
    clusterAffinity: Option[Long]
  ): Future[Long] =
    client.transaction { tx =>
      val now = Time.now.inMillis
      val columns =
        "tag, description, owner_id, datatype, origin, visibility, storage_class, retention_ms, schedule_ms, preferred_cluster, created_at, updated_at"
      val params = columns.split(',').map(_ => "?").mkString(", ")
      for {
        _ <- tx
          .prepare(
            s"INSERT INTO datasets($columns) VALUES($params)"
          )
          .modify(
            tag,
            description,
            owner,
            objectMapper.writeValueAsString(datatype),
            origin.name,
            visibility.name,
            storageClass.name,
            retention.map(_.inMillis),
            schedule.map(_.inMillis),
            clusterAffinity,
            now,
            now
          )
        id <- tx.select("SELECT LAST_INSERT_ID() as id")(_.longOrZero("id")).map(_.head)
      } yield id
    }

  override def update(
    datasetId: Long,
    description: String,
    retention: Option[Duration],
    schedule: Option[Duration],
    visibility: Visibility
  ): Future[Unit] = client
    .prepare(
      "UPDATE datasets SET description = ?, retention_ms = ?, schedule_ms = ?, visibility = ?, updated_at = ? WHERE id = ?"
    )
    .modify(
      description,
      retention.map(_.inMillis),
      schedule.map(_.inMillis),
      visibility.name,
      Time.now.inMillis,
      datasetId
    )
    .unit
}
