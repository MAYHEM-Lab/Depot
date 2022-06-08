package wtf.knc.depot.dao

import com.twitter.finagle.mysql.{Client, Row, Transactions}
import com.twitter.inject.Logging
import com.twitter.util.Future
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.model.{Entity, Role}

trait EntityDAO {
  def byId(id: Long): Future[Option[Entity]]
  def byName(name: String): Future[Option[Entity]]
  def byGithubId(githubId: Long): Future[Option[Long]]
  def searchUsers(name: String): Future[Seq[Entity]]
  def searchAll(name: String): Future[Seq[Entity]]

  def createWithGithub(username: String, githubId: Long, accessKey: String, secretKey: String): Future[Long]

  def createOrganization(name: String, ownerId: Long, accessKey: String, secretKey: String): Future[Long]

  def addMember(organizationId: Long, userId: Long, role: Role): Future[Unit]
  def removeMember(organizationId: Long, userId: Long): Future[Unit]
  def members(organizationId: Long): Future[Map[Long, Role]]
  def whereMember(userId: Long): Future[Seq[Entity]]

  def keys(id: Long): Future[(String, String)]
}

@Singleton
class MysqlEntityDAO @Inject() (
  client: Client with Transactions
) extends EntityDAO
  with Logging {

  private def extract(r: Row): Entity = {
    val id = r.longOrZero("id")
    val name = r.stringOrNull("name")
    val createdAt = r.longOrZero("created_at")
    r.stringOrNull("type") match {
      case "User" => Entity.User(id, name, createdAt)
      case "Organization" => Entity.Organization(id, name, createdAt)
    }
  }

  override def searchAll(name: String): Future[Seq[Entity]] = client
    .prepare("SELECT * FROM entities WHERE name LIKE ? LIMIT 50")
    .select(s"$name%")(extract)

  override def searchUsers(name: String): Future[Seq[Entity]] = client
    .prepare("SELECT * FROM entities WHERE name LIKE ? AND type = 'User' LIMIT 50")
    .select(s"$name%")(extract)

  override def keys(id: Long): Future[(String, String)] = client
    .prepare("SELECT access_key, secret_key FROM entities WHERE id = ?")
    .select(id)(r => r.stringOrNull("access_key") -> r.stringOrNull("secret_key"))
    .map(_.head)

  override def createOrganization(name: String, ownerId: Long, accessKey: String, secretKey: String): Future[Long] =
    client.transaction { tx =>
      val now = System.currentTimeMillis
      for {
        _ <- tx
          .prepare(
            "INSERT INTO entities(name, type, access_key, secret_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
          )
          .modify(name, "Organization", accessKey, secretKey, now, now)

        organizationId <- tx
          .select("SELECT LAST_INSERT_ID() as id")(_.longOrZero("id"))
          .map(_.head)

        _ <- tx
          .prepare("INSERT INTO organization_members(organization_id, member_id, role) VALUES(?, ?, ?)")
          .modify(organizationId, ownerId, Role.Owner.name)
          .unit
      } yield organizationId
    }

  override def addMember(organizationId: Long, userId: Long, role: Role): Future[Unit] = client
    .prepare("REPLACE INTO organization_members(organization_id, member_id, role) VALUES(?, ?, ?)")
    .modify(organizationId, userId, role.name)
    .unit

  override def removeMember(organizationId: Long, userId: Long): Future[Unit] = client
    .prepare("DELETE FROM organization_members WHERE organization_id = ? and member_id = ?")
    .modify(organizationId, userId)
    .unit

  override def members(organizationId: Long): Future[Map[Long, Role]] = client
    .prepare("SELECT * FROM organization_members WHERE organization_id = ? ORDER BY role ASC")
    .select(organizationId)(r => r.longOrZero("member_id") -> Role.parse(r.stringOrNull("role")))
    .map(_.toMap)

  override def whereMember(userId: Long): Future[Seq[Entity]] = client
    .prepare(
      "SELECT id, name, type, created_at, updated_at FROM organization_members JOIN entities ON entities.id = organization_members.organization_id AND organization_members.member_id = ?"
    )
    .select(userId)(extract)

  override def byId(id: Long): Future[Option[Entity]] = client
    .prepare("SELECT * FROM entities WHERE id = ?")
    .select(id)(extract)
    .map(_.headOption)

  override def byName(name: String): Future[Option[Entity]] = client
    .prepare("SELECT * FROM entities WHERE name = ?")
    .select(name)(extract)
    .map(_.headOption)

  override def byGithubId(githubId: Long): Future[Option[Long]] = client
    .prepare("SELECT * FROM github_user_link WHERE github_id = ?")
    .select(githubId)(_.longOrZero("user_id"))
    .map(_.headOption)

  override def createWithGithub(username: String, githubId: Long, accessKey: String, secretKey: String): Future[Long] =
    client.transaction { tx =>
      val now = System.currentTimeMillis
      tx
        .prepare(
          "INSERT INTO entities(name, type, access_key, secret_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
        )
        .modify(username, "User", accessKey, secretKey, now, now)
        .flatMap { _ =>
          tx
            .select("SELECT LAST_INSERT_ID() as id")(_.longOrZero("id"))
            .map(_.head)
        }
        .flatMap { userId =>
          tx
            .prepare("INSERT INTO github_user_link(user_id, github_id) VALUES (?, ?)")
            .modify(userId, githubId)
            .map(_ => userId)
        }
    }
}
