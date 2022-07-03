package wtf.knc.depot.message

import com.rabbitmq.client.{Channel, DeliverCallback, Delivery}
import com.twitter.inject.Logging
import com.twitter.inject.annotations.Flag
import com.twitter.inject.utils.Handler
import com.twitter.util.jackson.ScalaObjectMapper
import com.twitter.util.{Closable, Duration, Future, Time}
import javax.inject.{Inject, Singleton}

import scala.collection.mutable

trait MessageHandler {
  def apply(message: Message): Future[Unit]
}

trait Subscriber extends Handler with Closable {
  private[message] val handlers = mutable.ListBuffer.empty[MessageHandler]

  def subscribe(handler: MessageHandler): Closable = synchronized {
    handlers += handler
    Closable.make { _ =>
      synchronized {
        handlers -= handler
        Future.Done
      }
    }
  }
}

@Singleton
class RabbitMQSubscriber @Inject() (
  @Flag("rabbitmq.queue") queue: String,
  @Flag("rabbitmq.error_delay") errorDelay: Duration,
  publisher: Publisher,
  channel: Channel,
  objectMapper: ScalaObjectMapper
) extends Subscriber
  with Logging {
  private var consumerTag = ""

  private def receive(consumer: String, delivery: Delivery): Unit = {
    val seqNo = delivery.getEnvelope.getDeliveryTag
    val bytes = delivery.getBody
    println(new String(bytes))
    val message = objectMapper.parse[Message](bytes)
    val runHandlers = Future.join(handlers.map(_(message)))
    logger.info(s"Received message $seqNo on $consumer")

    runHandlers
      .onSuccess { _ => logger.info(s"Handled message $seqNo") }
      .rescue { case ex: Throwable =>
        logger.error(s"Failed to handle message $seqNo", ex)
        publisher.publish(message, errorDelay)
      }
      .ensure { channel.basicAck(seqNo, false) }
  }

  override def close(deadline: Time): Future[Unit] = {
    if (consumerTag != "") {
      channel.basicCancel(consumerTag)
      channel.close()
    }
    Future.Done
  }

  override def handle(): Unit = {
    consumerTag = channel.basicConsume(
      queue,
      false,
      receive: DeliverCallback,
      (tag: String) => {
        logger.info(s"Consumer $tag has been cancelled. Retrying...")
        handle()
      }
    )
    logger.info(s"Started consumer $consumerTag")
  }

}
