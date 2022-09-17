package wtf.knc.depot.service

import com.twitter.conversions.StorageUnitOps._
import com.twitter.util.Future
import wtf.knc.depot.dao._
import wtf.knc.depot.model._
import wtf.knc.depot.notebook.NotebookStore
import wtf.knc.depot.service.Storage.{NotebookFootprint, SegmentFootprint}

import java.util.concurrent.ConcurrentHashMap
import javax.inject.{Inject, Singleton}

object Storage {
  case class SegmentFootprint(
    datasetOwner: String,
    datasetTag: String,
    segmentVersion: Long,
    bytes: Long,
    state: SegmentState,
    cause: RetentionCause
  )

  case class NotebookFootprint(
    notebookTag: String,
    bytes: Long
  )

  case class Allocation(
    allocatedBytes: Long
  )
  case class Usage(
    usedBytes: Long,
    segments: Seq[SegmentFootprint],
    notebooks: Seq[NotebookFootprint]
  )
}

case class QuotaAllocation(storage: Storage.Allocation)
case class QuotaUsage(storage: Storage.Usage)

@Singleton
class QuotaService @Inject() (
  entityDAO: EntityDAO,
  datasetDAO: DatasetDAO,
  segmentDAO: SegmentDAO,
  retentionDAO: RetentionDAO,
  notebookStore: NotebookStore,
  notebookDAO: NotebookDAO
) {

  def allocatedQuota(entityId: Long): Future[QuotaAllocation] = {
    Future.value(QuotaAllocation(Storage.Allocation(512.megabytes.bytes)))
  }

  private def notebookFootprints(entityId: Long): Future[Seq[NotebookFootprint]] = {
    notebookDAO.byOwner(entityId).flatMap { notebooks =>
      val byNotebook = notebooks.map { notebook =>
        notebookStore.size(notebook.tag).map { size => NotebookFootprint(notebook.tag, size) }
      }
      Future.collect(byNotebook)
    }
  }

  private def segmentFootprints(entityId: Long): Future[Seq[SegmentFootprint]] = {
    val entityCache = new ConcurrentHashMap[Long, Future[Entity]]()
    val segmentCache = new ConcurrentHashMap[Long, Future[Segment]]()
    val datasetCache = new ConcurrentHashMap[Long, Future[Dataset]]()
    retentionDAO.listByHolder(entityId).flatMap { retentions =>
      val loadRetentions = retentions.map { r =>
        val loadSegment = segmentCache.computeIfAbsent(r.segmentId, segmentDAO.byId)
        val loadDataset = loadSegment.flatMap { segment =>
          datasetCache.computeIfAbsent(segment.datasetId, datasetDAO.byId)
        }
        val loadOwner = loadDataset.flatMap { dataset =>
          entityCache.computeIfAbsent(dataset.ownerId, i => entityDAO.byId(i).map(_.get))
        }
        Future.join(loadSegment, loadDataset, loadOwner).map { case (segment, dataset, owner) =>
          SegmentFootprint(owner.name, dataset.tag, segment.version, r.shallowSize, r.state, r.cause)
        }
      }
      Future.collect(loadRetentions)
    }
  }

  def consumedQuota(entityId: Long): Future[QuotaUsage] =
    for {
      notebooks <- notebookFootprints(entityId)
      segments <- segmentFootprints(entityId)
    } yield {
      val segmentBytes = segments
        .groupBy(s => (s.datasetOwner, s.datasetTag, s.segmentVersion))
        .values
        .map(_.head.bytes)
        .sum
      val notebookBytes = notebooks.map(_.bytes).sum
      val storage = Storage.Usage(notebookBytes + segmentBytes, segments, notebooks)
      QuotaUsage(storage)
    }

}
