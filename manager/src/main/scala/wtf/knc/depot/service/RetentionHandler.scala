package wtf.knc.depot.service

import com.twitter.conversions.DurationOps._
import com.twitter.util.logging.Logging
import com.twitter.util.{Future, Time}
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.dao.{DatasetDAO, EntityDAO, SegmentDAO}
import wtf.knc.depot.message.{Message, Publisher}
import wtf.knc.depot.model.{Dataset, Entity, Retainer, RetentionCause, Segment}

@Singleton
class RetentionHandler @Inject() (
  datasetDAO: DatasetDAO,
  entityDAO: EntityDAO,
  segmentDAO: SegmentDAO,
  cloudService: CloudService,
  publisher: Publisher
) extends Logging {

  private def pruneSegment(owner: Entity, dataset: Dataset, segment: Segment): Future[Unit] = {
    // TODO: need to remove all refs held by this segment
    val release = segmentDAO.inputs(segment.id).flatMap { inputs =>
      val releaseInputs = inputs.map { input =>
        segmentDAO.refsBySegment(input.sourceSegmentId).map { refs =>
          refs
            .filter {
              case Retainer(_, _, _, _, RetentionCause.Dependency(owner.name, dataset.tag, segment.version)) => true
              case _ => false
            }
        }
      }
      Future
        .collect(releaseInputs)
        .map(_.flatten)
        .flatMap { refs =>
          logger.info(s"Releasing ${refs.length} references held by segment $segment")
          Future.join(refs.map(segmentDAO.releaseRef))
        }
    }

    val reset = release.before {
      segmentDAO.data(segment.id).flatMap {
        case Some(data) =>
          logger.info(s"Reclaiming persisted data at ${data.path}")
          cloudService.reclaimPath(data.path)
        case _ =>
          logger.info(s"No persisted data to reclaim for ${segment.id}")
          Future.Done
      }
    }

    reset.before {
      logger.info(s"Deleting segment ${segment.id}")
      segmentDAO.delete(segment.id)
    }
  }

  private def pruneDataset(datasetId: Long): Future[Unit] = {
    datasetDAO.byId(datasetId).flatMap { dataset =>
      entityDAO.byId(dataset.ownerId).flatMap {
        case Some(owner) =>
          logger.info(s"Pruning dataset ${owner.name}/${dataset.tag}")
          segmentDAO.list(datasetId).flatMap { segments =>
            val loadSegments = segments.map { segment =>
              segmentDAO.refsBySegment(segment.id).flatMap { refs =>
                if (refs.isEmpty) {
                  logger.info(s"Segment ${segment.id} has no inbound references, pruning")
                  pruneSegment(owner, dataset, segment)
                } else {
                  logger.info(s"Segment ${segment.id} still has ${refs.size} inbound references")
                  Future.Done
                }
              }
            }
            Future.join(loadSegments)
          }
        case _ => throw new IllegalArgumentException(s"No owner for dataset $datasetId")
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
              segmentDAO.refsBySegment(segment.id).flatMap { retainers =>
                val ttls = retainers.filter { r =>
                  r.cause match {
                    case RetentionCause.TTL(createdAt) if createdAt + retention.inMillis < Time.now.inMillis => true
                    case _ => false
                  }
                }
                Future.join(ttls.map(segmentDAO.releaseRef))
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
