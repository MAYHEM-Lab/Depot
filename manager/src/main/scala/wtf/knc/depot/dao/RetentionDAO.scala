package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.util.Future
import com.twitter.util.jackson.ScalaObjectMapper
import wtf.knc.depot.model.{Retainer, RetentionCause, SegmentState}

import javax.inject.{Inject, Singleton}

trait RetentionDAO {
  def create(segmentId: Long, who: Long, state: SegmentState, shallowSize: Long, cause: RetentionCause): Future[Unit]
  def updateState(segmentId: Long, state: SegmentState): Future[Unit]
  def updateSize(segmentId: Long, shallowSize: Long): Future[Unit]
  def release(retainer: Retainer): Future[Unit]
  def listByHolder(who: Long): Future[Seq[Retainer]]
  def listBySegment(segmentId: Long): Future[Seq[Retainer]]
}

@Singleton
class MysqlRetentionDAO @Inject() (
  client: Client with Transactions,
  objectMapper: ScalaObjectMapper
) extends RetentionDAO {
  private def extract(row: Row): Retainer = Retainer(
    row.longOrZero("segment_id"),
    row.longOrZero("holder"),
    SegmentState.parse(row.stringOrNull("state")),
    row.longOrZero("shallow_size"),
    objectMapper.parse[RetentionCause](row.stringOrNull("cause"))
  )

  override def create(
    segmentId: Long,
    who: Long,
    state: SegmentState,
    shallowSize: Long,
    cause: RetentionCause
  ): Future[Unit] = client
    .prepare("INSERT INTO segment_retentions(segment_id, holder, state, shallow_size, cause) VALUES (?, ?, ?, ?, ?)")
    .modify(segmentId, who, state.name, shallowSize, objectMapper.writeValueAsString(cause))
    .unit

  override def updateState(segmentId: Long, state: SegmentState): Future[Unit] = client
    .prepare("UPDATE segment_retentions SET state = ? where segment_id = ?")
    .modify(state.name, segmentId)
    .unit

  override def updateSize(segmentId: Long, size: Long): Future[Unit] = client
    .prepare("UPDATE segment_retentions SET shallow_size = ? where segment_id = ?")
    .modify(size, segmentId)
    .unit

  override def release(retainer: Retainer): Future[Unit] = client
    .prepare("DELETE FROM segment_retentions WHERE segment_id = ? AND holder = ? AND cause = ?")
    .modify(retainer.segmentId, retainer.holder, objectMapper.writeValueAsString(retainer.cause))
    .unit

  override def listByHolder(who: Long): Future[Seq[Retainer]] = client
    .prepare("SELECT * FROM segment_retentions WHERE holder = ? ORDER BY shallow_size DESC")
    .select(who)(extract)

  override def listBySegment(segmentId: Long): Future[Seq[Retainer]] = client
    .prepare("SELECT * FROM segment_retentions WHERE segment_id = ? ORDER BY shallow_size DESC")
    .select(segmentId)(extract)

}
