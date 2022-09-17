package wtf.knc.depot.module

import com.google.inject.Provides
import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.{Client, Transactions}
import com.twitter.inject.annotations.Flag
import com.twitter.inject.{Injector, TwitterModule}
import javax.inject.Singleton
import wtf.knc.depot.SchemaMigration
import wtf.knc.depot.dao._

object MysqlModule extends TwitterModule {
  flag[String]("mysql.host", "localhost", "MySQL server host")
  flag[Int]("mysql.port", 3306, "MySQL server port")
  flag[String]("mysql.database", "depot", "MySQL database")

  flag[String]("mysql.username", "root", "MySQL server username")
  flag[String]("mysql.password", "root", "MySQL server password")

  override def configure(): Unit = {
    bind[DatasetDAO].to[MysqlDatasetDAO]
    bind[TransformationDAO].to[MysqlTransformationDAO]
    bind[SegmentDAO].to[MysqlSegmentDAO]
    bind[GraphDAO].to[MysqlGraphDAO]
    bind[NotebookDAO].to[MysqlNotebookDAO]
    bind[EntityDAO].to[MysqlEntityDAO]
    bind[ClusterDAO].to[MysqlClusterDAO]
    bind[RetentionDAO].to[MysqlRetentionDAO]
  }

  override def singletonStartup(injector: Injector): Unit = {
    injector.instance[SchemaMigration].handle()
  }

  @Provides
  @Singleton
  def mysqlClient(
    @Flag("mysql.host") host: String,
    @Flag("mysql.port") port: Int,
    @Flag("mysql.database") database: String,
    @Flag("mysql.username") username: String,
    @Flag("mysql.password") password: String
  ): Client with Transactions = Mysql.client
    .withCredentials(username, password)
    .withDatabase(database)
    .withSessionPool
    .minSize(16)
    .withSessionPool
    .maxSize(32)
    .newRichClient(s"$host:$port", "mysql-client")

}
