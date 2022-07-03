package wtf.knc.depot.service

import com.twitter.util.logging.Logging
import com.twitter.util.{Future, Time}
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.dao.{DatasetDAO, SegmentDAO}
import wtf.knc.depot.message.{Message, Publisher}
import wtf.knc.depot.model.SegmentState

@Singleton
class RetentionHandler @Inject() (
  datasetDAO: DatasetDAO,
  segmentDAO: SegmentDAO,
  cloudService: CloudService,
  publisher: Publisher
) extends Logging {

  def pruneSegment(segmentId: Long): Future[Unit] = {
    segmentDAO
      .update(segmentId, SegmentState.Released) { implicit ctx =>
        { case (_, _) =>
          segmentDAO.outputs(segmentId).flatMap {
            case Nil =>
              val reset = segmentDAO.data(segmentId).flatMap {
                case Some(data) =>
                  logger.info(s"Reclaiming persisted data at ${data.path}")
                  cloudService.reclaimPath(data.path)
                case _ =>
                  logger.info(s"No persisted data to reclaim for $segmentId")
                  Future.Done
              }

              reset.before {
                logger.info(s"Deleting segment $segmentId")
                segmentDAO.delete(segmentId)
              }
            case _ =>
              logger.info(s"Segment $segmentId still has inbound references")
              Future.Done
          }
        }
      }
  }

  def checkRetention(datasetId: Long, updatedAt: Long): Future[Unit] = {
    datasetDAO.byId(datasetId).flatMap { dataset =>
      if (updatedAt == dataset.updatedAt) {
        dataset.retention match {
          case Some(retention) =>
            logger.info(s"Pruning expired segments for dataset ${dataset.tag}")
            segmentDAO
              .list(datasetId)
              .map { segments =>
                val latestMaterialized = segments
                  .filter(_.state == SegmentState.Materialized)
                  .maxByOption(_.version)
                  .map(_.id)

                segments
                  .filter(_.id != latestMaterialized.getOrElse(0L))
                  .filter(_.createdAt < (Time.now - retention).inMillis)
              }
              .flatMap { segments =>
                logger.info(s"Identified segments [${segments.mkString(", ")}] as expired")
                val prune = segments.map(_.id).map(pruneSegment)
                Future.join(prune)
              }
              .before {
                publisher.publish(Message.DatasetPrune(datasetId, updatedAt), retention)
              }
          case _ =>
            logger.info(s"Dataset ${dataset.tag} has no retention policy")
            Future.Done
        }
      } else {
        logger.info(s"Ignoring old retention timeout for ${dataset.tag}")
        Future.Done
      }
    }
  }
}
