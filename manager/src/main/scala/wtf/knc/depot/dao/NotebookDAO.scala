package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.util.{Future, Time}
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.model.Notebook

trait NotebookDAO {
  def byTag(tag: String): Future[Option[Notebook]]
  def byOwner(ownerId: Long): Future[Seq[Notebook]]
  def create(tag: String, ownerId: Long): Future[Unit]
}

@Singleton
class MysqlNotebookDAO @Inject() (
  client: Client with Transactions
) extends NotebookDAO {
  private def extract(r: Row): Notebook = {
    val tag = r.stringOrNull("tag")
    val owner = r.longOrZero("owner_id")
    val created_at = r.longOrZero("created_at")
    val updated_at = r.longOrZero("updated_at")
    Notebook(tag, owner, created_at, updated_at)
  }

  override def byOwner(ownerId: Long): Future[Seq[Notebook]] = client
    .prepare("SELECT * FROM notebooks WHERE owner_id = ?")
    .select(ownerId)(extract)

  override def byTag(tag: String): Future[Option[Notebook]] = client
    .prepare("SELECT * FROM notebooks WHERE tag = ?")
    .select(tag)(extract)
    .map(_.headOption)

  override def create(tag: String, ownerId: Long): Future[Unit] = client.transaction { tx =>
    val now = Time.now.inMillis
    tx
      .prepare("INSERT INTO notebooks(tag, owner_id, created_at, updated_at) VALUES(?, ?, ?, ?)")
      .modify(tag, ownerId, now, now)
      .unit
  }
}
