package wtf.knc.depot.message

import java.util.concurrent.{ConcurrentSkipListMap, Executors}

import com.rabbitmq.client.{AMQP, Channel}
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.inject.Logging
import com.twitter.inject.annotations.Flag
import com.twitter.inject.utils.Handler
import com.twitter.util._
import com.twitter.util.jackson.ScalaObjectMapper
import javax.inject.{Inject, Singleton}

import scala.jdk.CollectionConverters._

trait Publisher extends Handler with Closable {
  def publish(message: Message, delay: Duration = Duration.Zero): Future[Unit]
}

@Singleton
class RabbitMQPublisher @Inject() (
  @Flag("rabbitmq.queue") queue: String,
  @Flag("rabbitmq.exchange") exchange: String,
  channel: Channel,
  objectMapper: ScalaObjectMapper
) extends Publisher
  with Logging {
  private val publisherPool = new ExecutorServiceFuturePool(
    Executors.newFixedThreadPool(1, new NamedPoolThreadFactory("rabbitmq-publisher"))
  )

  private val outstanding = new ConcurrentSkipListMap[Long, Promise[Unit]]().asScala

  private def satisfy(success: Boolean)(seqNo: Long, multiple: Boolean): Unit = {
    val targets = if (multiple) {
      outstanding.takeWhile(_._1 <= seqNo)
    } else {
      outstanding.filter(_._1 == seqNo)
    }
    targets.values.foreach { p =>
      if (success) {
        logger.info(s"Successfully published $seqNo, multiple: $multiple")
        p.setDone()
      } else {
        p.setException(new Exception(s"Publish failed"))
      }
    }
    outstanding --= targets.keys
  }

  override def handle(): Unit = Await.result {
    publisherPool {
      synchronized {
        channel.confirmSelect()
        channel.addConfirmListener(
          satisfy(success = true) _,
          satisfy(success = false) _
        )
      }
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    publisherPool.executor.shutdownNow()
    Future.Done
  }

  override def publish(mesage: Message, delay: Duration): Future[Unit] = {
    val promise = Promise[Unit]()
    val payload = objectMapper.writeValueAsBytes(mesage)
    publisherPool {
      synchronized {
        val seqNo = channel.getNextPublishSeqNo
        outstanding(seqNo) = promise
        val headers = Map("x-delay" -> delay.inMillis.asInstanceOf[Object]).asJava
        val props = new AMQP.BasicProperties.Builder().headers(headers).build()
        channel.basicPublish(exchange, queue, props, payload)
        logger.info(s"Published message $seqNo")
      }
    }
    promise
  }
}
