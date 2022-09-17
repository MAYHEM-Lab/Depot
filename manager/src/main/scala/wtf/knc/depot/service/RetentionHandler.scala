package wtf.knc.depot.service

import com.twitter.conversions.DurationOps._
import com.twitter.util.logging.Logging
import com.twitter.util.{Future, Time}
import wtf.knc.depot.dao.{DatasetDAO, RetentionDAO, SegmentDAO}
import wtf.knc.depot.message.{Message, Publisher}
import wtf.knc.depot.model.{RetentionCause, Segment}

import javax.inject.{Inject, Singleton}

@Singleton
class RetentionHandler @Inject() (
  datasetDAO: DatasetDAO,
  segmentDAO: SegmentDAO,
  retentionDAO: RetentionDAO,
  cloudService: CloudService,
  publisher: Publisher
) extends Logging {

  private def pruneSegment(segment: Segment): Future[Unit] = {
    val reset = segmentDAO.data(segment.id).flatMap {
      case Some(data) =>
        logger.info(s"Reclaiming persisted data at ${data.path}")
        cloudService.reclaimPath(data.path)
      case _ =>
        logger.info(s"No persisted data to reclaim for ${segment.id}")
        Future.Done
    }

    reset.before {
      logger.info(s"Deleting segment ${segment.id}")
      segmentDAO.delete(segment.id)
    }
  }

  private def pruneDataset(datasetId: Long): Future[Unit] = {
    datasetDAO.byId(datasetId).flatMap { dataset =>
      logger.info(s"Pruning dataset ${dataset.tag}")
      segmentDAO.list(datasetId).flatMap { segments =>
        val loadSegments = segments.map { segment =>
          retentionDAO.listBySegment(segment.id).flatMap { refs =>
            if (refs.isEmpty) {
              logger.info(s"Segment ${segment.id} has no inbound references, pruning")
              pruneSegment(segment)
            } else {
              logger.info(s"Segment ${segment.id} still has ${refs.size} inbound references")
              Future.Done
            }
          }
        }
        Future.join(loadSegments)
      }
    }
  }

  private def releaseExpired(datasetId: Long): Future[Unit] = {
    datasetDAO.byId(datasetId).flatMap { dataset =>
      dataset.retention match {
        case Some(retention) =>
          logger.info(s"Releasing expired retentions for ${dataset.tag}")
          segmentDAO.list(datasetId).flatMap { segments =>
            val releaseSegments = segments.map { segment =>
              retentionDAO.listBySegment(segment.id).flatMap { retainers =>
                val ttls = retainers.filter { r =>
                  r.cause match {
                    case RetentionCause.TTL(createdAt) if createdAt + retention.inMillis < Time.now.inMillis => true
                    case _ => false
                  }
                }
                Future.join(ttls.map(retentionDAO.release))
              }
            }
            Future.join(releaseSegments)
          }
        case _ =>
          logger.info(s"Dataset ${dataset.tag} has no retention configured")
          Future.Done
      }
    }
  }

  def checkRetention(datasetId: Long, updatedAt: Long): Future[Unit] = {
    releaseExpired(datasetId)
      .before {
        pruneDataset(datasetId)
      }
      .before {
        publisher.publish(Message.DatasetPrune(datasetId, updatedAt), 2.minutes)
      }
      .handle { case _: NoSuchElementException =>
        logger.info(s"Ignoring message for dataset $datasetId")
      }
  }
}
