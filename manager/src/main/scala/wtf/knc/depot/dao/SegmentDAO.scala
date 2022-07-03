package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.util.jackson.ScalaObjectMapper
import com.twitter.util.{Future, Time}
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.dao.SegmentDAO.SegmentNotFoundException
import wtf.knc.depot.model._

object SegmentDAO {
  case class SegmentNotFoundException(segmentId: Long) extends Exception
}

trait SegmentDAO extends DAO {
  def byVersion(datasetId: Long, version: Long)(implicit ctx: Ctx = defaultCtx): Future[Option[Segment]]
  def byId(segmentId: Long)(implicit ctx: Ctx = defaultCtx): Future[Segment]

  def inputs(segmentId: Long)(implicit ctx: Ctx = defaultCtx): Future[Seq[SegmentInput]]
  def outputs(segmentId: Long)(implicit ctx: Ctx = defaultCtx): Future[Seq[SegmentInput]]

  def data(segmentId: Long)(implicit ctx: Ctx = defaultCtx): Future[Option[SegmentData]]
  def setData(segmentId: Long, data: SegmentData)(implicit ctx: Ctx = defaultCtx): Future[Unit]

  def list(datasetId: Long)(implicit ctx: Ctx = defaultCtx): Future[Seq[Segment]]

  def count(datasetId: Long)(implicit ctx: Ctx = defaultCtx): Future[Long]
  def size(datasetId: Long)(implicit ctx: Ctx = defaultCtx): Future[Long]

  def make(datasetId: Long)(implicit ctx: Ctx = defaultCtx): Future[Long]
  def addInputs(segmentId: Long, inputs: Map[String, Long])(implicit ctx: Ctx = defaultCtx): Future[Unit]

  def delete(segmentId: Long)(implicit ctx: Ctx = defaultCtx): Future[Unit]

  def transitions(segmentId: Long)(implicit ctx: Ctx = defaultCtx): Future[Map[Long, Transition]]
  def recordTransition(segmentId: Long, transition: Transition)(implicit ctx: Ctx = defaultCtx): Future[Unit]

  def update(segmentId: Long, to: SegmentState)(fx: Ctx => (SegmentState, SegmentState) => Future[Unit]): Future[Unit]
}

@Singleton
class MysqlSegmentDAO @Inject() (
  override val client: Client with Transactions,
  objectMapper: ScalaObjectMapper
) extends SegmentDAO
  with MysqlDAO {

  def extractData(r: Row): SegmentData = SegmentData(
    r.stringOrNull("path"),
    r.stringOrNull("checksum"),
    r.longOrZero("size"),
    r.longOrZero("row_count"),
    objectMapper.parse[Seq[Seq[String]]](r.stringOrNull("sample"))
  )

  def extractInput(r: Row): SegmentInput = SegmentInput(
    r.longOrZero("target_segment_id"),
    r.longOrZero("source_segment_id"),
    r.stringOrNull("binding")
  )

  def extractSegment(r: Row): Segment = Segment(
    r.longOrZero("id"),
    r.longOrZero("dataset_id"),
    r.longOrZero("version"),
    SegmentState.parse(r.stringOrNull("state")),
    r.longOrZero("created_at"),
    r.longOrZero("updated_at")
  )

  override def byId(segmentId: Long)(implicit ctx: MysqlCtx): Future[Segment] = ctx { tx =>
    tx.prepare("SELECT * FROM segments WHERE id = ?")
      .select(segmentId)(extractSegment)
      .map(_.head)
  }

  override def byVersion(datasetId: Long, version: Long)(implicit ctx: MysqlCtx): Future[Option[Segment]] = ctx { tx =>
    tx.prepare("SELECT * FROM segments WHERE dataset_id = ? AND version = ?")
      .select(datasetId, version)(extractSegment)
      .map(_.headOption)
  }

  override def list(datasetId: Long)(implicit ctx: MysqlCtx): Future[Seq[Segment]] = ctx { tx =>
    tx.prepare("SELECT * FROM segments WHERE dataset_id = ? ORDER BY version DESC")
      .select(datasetId)(extractSegment)
  }

  override def inputs(segmentId: Long)(implicit ctx: MysqlCtx): Future[Seq[SegmentInput]] = ctx { tx =>
    tx.prepare("SELECT * FROM segment_inputs WHERE target_segment_id = ?")
      .select(segmentId)(extractInput)
  }

  override def outputs(segmentId: Long)(implicit ctx: MysqlCtx): Future[Seq[SegmentInput]] = ctx { tx =>
    tx.prepare("SELECT * FROM segment_inputs WHERE source_segment_id = ?")
      .select(segmentId)(extractInput)
  }

  override def data(segmentId: Long)(implicit ctx: MysqlCtx): Future[Option[SegmentData]] = ctx { tx =>
    tx.prepare("SELECT * FROM segment_data WHERE segment_id = ?")
      .select(segmentId)(extractData)
      .map(_.headOption)
  }

  override def setData(segmentId: Long, data: SegmentData)(implicit ctx: MysqlCtx): Future[Unit] = ctx { tx =>
    tx.prepare(
      "INSERT INTO segment_data(segment_id, path, checksum, size, row_count, sample) VALUES (?, ?, ?, ?, ?, ?)"
    ).modify(segmentId, data.path, data.checksum, data.size, data.rows, objectMapper.writeValueAsString(data.sample))
      .unit
  }

  override def count(datasetId: Long)(implicit ctx: MysqlCtx): Future[Long] = ctx { tx =>
    tx.prepare("SELECT CAST(COUNT(*) AS UNSIGNED INTEGER) AS count FROM segments WHERE dataset_id = ?")
      .select(datasetId)(_.longOrZero("count"))
      .map(_.head)
  }

  override def size(datasetId: Long)(implicit ctx: MysqlCtx): Future[Long] = ctx { tx =>
    tx.prepare(
      "SELECT CAST(SUM(segment_data.size) AS UNSIGNED INTEGER) AS size FROM segment_data JOIN segments ON segments.id = segment_data.segment_id AND segments.dataset_id = ?"
    ).select(datasetId)(_.longOrZero("size"))
      .map(_.head)
  }

  override def make(datasetId: Long)(implicit ctx: MysqlCtx): Future[Long] = ctx { tx =>
    val now = Time.now.inMillis
    tx.prepare("SELECT CAST(MAX(version) AS UNSIGNED INTEGER) AS max_version FROM segments WHERE dataset_id = ?")
      .select(datasetId)(_.longOrZero("max_version"))
      .map(_.head)
      .flatMap { maxVersion =>
        tx
          .prepare("INSERT INTO segments(dataset_id, version, state, created_at, updated_at) VALUES(?, ?, ?, ?, ?)")
          .modify(
            datasetId,
            maxVersion + 1,
            SegmentState.Initializing.name,
            now,
            now
          )
          .flatMap { _ =>
            // TODO: kc This may not get executed right after the previous if we are handling multiple segments
            tx
              .prepare("SELECT CAST(MAX(id) AS UNSIGNED INTEGER) AS id FROM segments WHERE dataset_id = ?")
              .select(datasetId)(_.longOrZero("id"))
              .map(_.head)
          }
      }
  }

  override def addInputs(segmentId: Long, inputs: Map[String, Long])(implicit ctx: MysqlCtx): Future[Unit] = ctx { tx =>
    val inserts = inputs.toSeq.map { case (binding, sourceSegmentId) =>
      tx
        .prepare("INSERT INTO segment_inputs(target_segment_id, source_segment_id, binding) VALUES(?, ?, ?)")
        .modify(segmentId, sourceSegmentId, binding)
        .unit
    }
    Future.join(inserts)
  }

  def transitions(segmentId: Long)(implicit ctx: MysqlCtx): Future[Map[Long, Transition]] = ctx { tx =>
    tx.prepare("SELECT * FROM segment_transitions WHERE segment_id = ?")
      .select(segmentId) { r =>
        r.longOrZero("created_at") -> objectMapper.parse[Transition](r.stringOrNull("transition"))
      }
      .map(_.toMap)
  }

  override def delete(segmentId: Long)(implicit ctx: Ctx = defaultCtx): Future[Unit] = ctx { tx =>
    tx.prepare("DELETE FROM segments WHERE id = ?")
      .modify(segmentId)
      .unit
  }

  def recordTransition(segmentId: Long, transition: Transition)(implicit ctx: MysqlCtx): Future[Unit] = ctx { tx =>
    tx
      .prepare("INSERT INTO segment_transitions(segment_id, transition, created_at) VALUES(?, ?, ?)")
      .modify(
        segmentId,
        objectMapper.writeValueAsString(transition),
        Time.now.inMillis
      )
      .unit
  }

  override def update(segmentId: Long, to: SegmentState)(
    fx: MysqlCtx => (SegmentState, SegmentState) => Future[Unit]
  ): Future[Unit] = client.session { session =>
    // todo: needs to be done in a transaction within the session

    for {
      _ <- session.query("SELECT count(*) FROM segments FOR UPDATE")
      currentState <- session
        .prepare("SELECT state FROM segments WHERE id = ?")
        .select(segmentId)(r => SegmentState.parse(r.stringOrNull("state")))
        .map(_.headOption.getOrElse(throw SegmentNotFoundException(segmentId)))
      _ <- session.transaction { tx =>
        tx
          .prepare("UPDATE segments SET state = ?, updated_at = ? WHERE id = ?")
          .modify(to.name, Time.now.inMillis, segmentId)
          .unit
          .before(fx(MuxMysqlCtx(tx))(currentState, to))
      }
    } yield ()
  }
}
