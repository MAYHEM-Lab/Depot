package wtf.knc.depot.service

import com.twitter.inject.Logging
import com.twitter.util._
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.message.{Message, Subscriber}

@Singleton
class MessageService @Inject() (
  transitionHandler: TransitionHandler,
  scheduleHandler: ScheduleHandler,
  subscriber: Subscriber
) extends Logging
  with Closable {
  private case class InvalidTransitionException() extends Exception

  private val subscription = subscriber.subscribe {
    case Message.SegmentTransition(segmentId, transition) =>
      transitionHandler.handleTransition(segmentId, transition)
    case Message.DatasetSchedule(datasetId) =>
      scheduleHandler.handleSchedule(datasetId)
    case msg => Future.exception(new Exception(s"Unrecognized message: $msg"))
  }

  /*
   * Watchdog message:
   * Check current state of version. If it is materialized, ack the message.
   * If it is await set is empty, start materialization and transit to Processing, ignore message.
   * If it is Processing and updated_at is old, transform the work again
   *  Otherwise ignore and let it come back to another worker later.
   */

  override def close(deadline: Time): Future[Unit] = subscription.close(deadline)
}
