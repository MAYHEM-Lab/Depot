package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.util.{Future, Time}
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.model._

trait GraphDAO {
  def make(targetDatasetId: Long, sourceDatasetId: Long, binding: String, mode: InputMode, valid: Boolean): Future[Unit]
  def updateEdge(targetDatasetId: Long, sourceDatasetId: Long, binding: String, valid: Boolean): Future[Unit]
  def out(datasetId: Long): Future[Seq[GraphEdge]]
  def in(datasetId: Long): Future[Seq[GraphEdge]]
}

@Singleton
class MysqlGraphDAO @Inject() (
  client: Client with Transactions
) extends GraphDAO {

  private object ModeSerde {
    def name(mode: InputMode): String = mode match {
      case InputMode.Ancilla => "ANCILLA"
      case InputMode.Trigger => "TRIGGER"
    }
    def from(str: String): InputMode = str match {
      case "ANCILLA" => InputMode.Ancilla
      case "TRIGGER" => InputMode.Trigger
      case _ => throw new IllegalArgumentException
    }
  }

  private def readEdge(r: Row): GraphEdge = {
    val targetId = r.longOrZero("target_dataset_id")
    val sourceId = r.longOrZero("source_dataset_id")
    val binding = r.stringOrNull("binding")
    val inputMode = ModeSerde.from(r.stringOrNull("input_mode"))
    val valid = r.booleanOrFalse("valid")
    val createdAt = r.longOrZero("created_at")
    val updatedAt = r.longOrZero("updated_at")
    GraphEdge(targetId, sourceId, binding, inputMode, valid, createdAt, updatedAt)
  }

  override def updateEdge(
    targetDatasetId: Long,
    sourceDatasetId: Long,
    binding: String,
    valid: Boolean
  ): Future[Unit] = {
    val now = Time.now.inMillis
    client
      .prepare(
        "UPDATE graph SET valid = ?, updated_at = ? WHERE target_dataset_id = ? AND source_dataset_id = ? AND binding = ?"
      )
      .modify(valid, now, targetDatasetId, sourceDatasetId, binding)
      .unit
  }

  def make(
    targetDatasetId: Long,
    sourceDatasetId: Long,
    binding: String,
    mode: InputMode,
    valid: Boolean
  ): Future[Unit] = {
    val now = Time.now.inMillis
    client
      .prepare(
        "INSERT INTO graph(target_dataset_id, source_dataset_id, binding, input_mode, valid, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)"
      )
      .modify(
        targetDatasetId,
        sourceDatasetId,
        binding,
        ModeSerde.name(mode),
        valid,
        now,
        now
      )
      .unit
  }

  override def out(datasetId: Long): Future[Seq[GraphEdge]] = client
    .prepare("SELECT * FROM graph WHERE source_dataset_id = ?")
    .select(datasetId)(readEdge)

  override def in(datasetId: Long): Future[Seq[GraphEdge]] = client
    .prepare("SELECT * FROM graph WHERE target_dataset_id = ?")
    .select(datasetId)(readEdge)
}
