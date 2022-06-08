package wtf.knc.depot

import com.twitter.finagle.mysql.{Client, Transactions}
import com.twitter.util.Future

package object dao {
  trait DAO {
    type Ctx
    def defaultCtx: Ctx
  }

  trait MysqlDAO extends DAO {
    sealed trait MysqlCtx {
      def apply[T](fx: Client => Future[T]): Future[T]
    }

    case class DefaultMysqlCtx(client: Client with Transactions) extends MysqlCtx {
      def apply[T](fx: Client => Future[T]): Future[T] = client.transaction(fx)
    }

    case class MuxMysqlCtx(client: Client) extends MysqlCtx {
      def apply[T](fx: Client => Future[T]): Future[T] = fx(client)
    }

    override type Ctx = MysqlCtx
    override def defaultCtx: MysqlCtx = DefaultMysqlCtx(client)
    val client: Client with Transactions
  }
}
