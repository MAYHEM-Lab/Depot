package wtf.knc.depot

import com.twitter.inject.annotations.Flag
import com.twitter.inject.utils.Handler
import javax.inject.{Inject, Singleton}
import org.flywaydb.core.Flyway

@Singleton
class SchemaMigration @Inject() (
  @Flag("mysql.host") host: String,
  @Flag("mysql.port") port: Int,
  @Flag("mysql.database") database: String,
  @Flag("mysql.username") username: String,
  @Flag("mysql.password") password: String
) extends Handler {
  override def handle(): Unit = {
    val flyway = Flyway.configure
      .dataSource(s"jdbc:mysql://$host:$port", username, password)
      .createSchemas(true)
      .schemas(database)
      .load()
    flyway.migrate()
  }
}
