package wtf.knc.depot.module

import com.google.inject.Provides
import com.rabbitmq.client.{Channel, ConnectionFactory}
import com.twitter.conversions.DurationOps._
import com.twitter.inject.annotations.Flag
import com.twitter.inject.{Injector, TwitterModule}
import com.twitter.util.{Await, Closable, Duration}
import javax.inject.Singleton
import wtf.knc.depot.message._

import scala.jdk.CollectionConverters._

object RabbitMQModule extends TwitterModule {
  flag[String]("rabbitmq.host", "localhost", "RabbitMQ server host")
  flag[Int]("rabbitmq.port", 5672, "RabbitMQ server port")
  flag[String]("rabbitmq.username", "guest", "RabbitMQ username")
  flag[String]("rabbitmq.password", "guest", "RabbitMQ password")

  flag[String](
    "rabbitmq.queue",
    "depot_queue",
    "RabbitMQ queue to use for messages"
  )

  flag[String](
    "rabbitmq.exchange",
    "depot_exchange",
    "RabbitMQ exchange to use to dispatch messages"
  )

  flag[Duration](
    "rabbitmq.error_delay",
    1.minute,
    "Duration to delay failed message requests before retrying"
  )

  override def configure(): Unit = {
    bind[Subscriber].to[RabbitMQSubscriber]
    bind[Publisher].to[RabbitMQPublisher]
  }

  override def singletonStartup(injector: Injector): Unit = {
    val publisher = injector.instance[Publisher]
    val subscriber = injector.instance[Subscriber]

    publisher.handle()
    subscriber.handle()
  }

  override def singletonShutdown(injector: Injector): Unit = {
    val publisher = injector.instance[Publisher]
    val subscriber = injector.instance[Subscriber]

    Await.result(Closable.all(publisher, subscriber).close())
  }

  @Singleton
  @Provides
  def channel(
    @Flag("rabbitmq.queue") queue: String,
    @Flag("rabbitmq.exchange") exchange: String,
    @Flag("rabbitmq.host") host: String,
    @Flag("rabbitmq.port") port: Int,
    @Flag("rabbitmq.username") username: String,
    @Flag("rabbitmq.password") password: String
  ): Channel = {
    val factory = new ConnectionFactory
    factory.setHost(host)
    factory.setPort(port)
    factory.setUsername(username)
    factory.setPassword(password)
    val connection = factory.newConnection
    val channel = connection.createChannel()
    channel.basicQos(1)
    channel.exchangeDeclare(
      exchange,
      "x-delayed-message",
      true,
      false,
      Map("x-delayed-type" -> "direct".asInstanceOf[Object]).asJava
    )
    channel.queueDeclare(queue, true, false, false, null)
    channel.queueBind(queue, exchange, queue)

    channel.confirmSelect()

    channel.addShutdownListener { ex =>
      logger.error(s"Channel shutdown", ex)
      sys.exit(1)
    }

    onExit {
      channel.close()
      connection.close()
    }
    channel
  }
}
