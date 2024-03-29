package wtf.knc.depot.service

import com.twitter.inject.Logging
import com.twitter.util.{Duration, Future, Time}
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.dao.{DatasetDAO, GraphDAO, SegmentDAO}
import wtf.knc.depot.message.{Message, Publisher}
import wtf.knc.depot.model.{InputMode, SegmentState, Transition, Trigger}

@Singleton
class ScheduleHandler @Inject() (
  datasetDAO: DatasetDAO,
  segmentDAO: SegmentDAO,
  graphDAO: GraphDAO,
  publisher: Publisher,
  transitionHandler: TransitionHandler
) extends Logging {
  private def materializeLatest(datasetId: Long): Future[Unit] = segmentDAO.list(datasetId).flatMap { segments =>
    segments
      .sortBy(_.version)
      .lastOption match {
      case Some(segment) if segment.state == SegmentState.Announced =>
        logger.info(s"Transitioning segment ${segment.id} of dataset $datasetId to Awaiting")
        publisher
          .publish(Message.SegmentTransition(segment.id, Transition.Await(Trigger.Scheduled(Time.now.inMillis))))
      case _ => Future.Done
    }
  }

  private def createNew(datasetId: Long): Future[Unit] = {
    graphDAO.in(datasetId).flatMap { edges =>
      val isolated = edges.forall(_.inputMode == InputMode.Ancilla)

      if (isolated) {
        logger.info(s"Dataset $datasetId is isolated, generating new segment")
        transitionHandler.createSegment(datasetId, Trigger.Scheduled(Time.now.inMillis))
      } else {
        logger.info(s"Dataset $datasetId is not isolated, not generating new segments")
        Future.Done
      }
    }
  }

  def handleSchedule(datasetId: Long, updatedAt: Long): Future[Unit] = {
    datasetDAO
      .byId(datasetId)
      .flatMap { dataset =>
        if (dataset.updatedAt == updatedAt) {
          logger.info(s"Handling schedule timeout for ${dataset.tag} [$datasetId]")

          for {
            _ <- materializeLatest(datasetId)
            _ <- createNew(datasetId)
            _ <- dataset.schedule.fold(Future.Done)(publisher.publish(Message.DatasetSchedule(datasetId, updatedAt), _))
          } yield ()
        } else {
          logger.info(s"Ignoring old schedule timeout for ${dataset.tag}")
          Future.Done
        }
      }
      .handle { case _: NoSuchElementException =>
        logger.info(s"Ignoring message for dataset $datasetId")
      }
  }
}
