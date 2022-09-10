package wtf.knc.depot.service

import com.twitter.util.Future
import javax.inject.{Inject, Singleton}
import com.twitter.conversions.StorageUnitOps._
import wtf.knc.depot.dao.{DatasetDAO, EntityDAO, NotebookDAO, SegmentDAO}
import wtf.knc.depot.model.{SegmentState, Transition, Trigger}
import wtf.knc.depot.notebook.NotebookStore
import wtf.knc.depot.service.Storage.NotebookFootprint

object Storage {
  case class SegmentFootprint(
    datasetOwner: String,
    datasetTag: String,
    segmentVersion: Long,
    bytes: Long
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
  notebookStore: NotebookStore,
  notebookDAO: NotebookDAO
) {

  def allocatedQuota(entityId: Long): Future[QuotaAllocation] = {
    Future.value(QuotaAllocation(Storage.Allocation(512.megabytes.bytes)))
  }

  // TODO: need to check datasets that the user materialized from others, as part of an export, integration, or notebook
  def consumedQuota(entityId: Long): Future[QuotaUsage] = entityDAO.byId(entityId).flatMap {
    case Some(entity) =>
      val notebooks = notebookDAO.byOwner(entityId).flatMap { notebooks =>
        val byNotebook = notebooks.map { notebook =>
          notebookStore.size(notebook.tag).map { size => NotebookFootprint(notebook.tag, size) }
        }
        Future.collect(byNotebook)
      }

      val segments = datasetDAO.byOwner(entityId).flatMap { datasets =>
        val byDataset = datasets.map { dataset =>
          segmentDAO.list(dataset.id).flatMap { segments =>
            val bySegment = segments.map { segment =>
              if (segment.state == SegmentState.Materialized) {
                segmentDAO.data(segment.id).flatMap {
                  case Some(data) =>
                    segmentDAO.transitions(segment.id).map { transitions =>
                      val isOwned = transitions.values.exists {
                        case Transition.Await(Trigger.Manual(_, `entityId`)) => true
                        case Transition.Materialize(_, Trigger.Manual(_, `entityId`)) => true
                        case _ => false
                      }
                      if (isOwned) {
                        Some(Storage.SegmentFootprint(entity.name, dataset.tag, segment.version, data.size))
                      } else {
                        None
                      }
                    }
                  case _ => Future.value(None)
                }
              } else {
                Future.value(None)
              }
            }
            Future.collect(bySegment).map(_.flatten)
          }
        }
        Future.collect(byDataset).map(_.flatten)
      }
      Future.join(notebooks, segments).map { case (n, s) =>
        val storage = Storage.Usage(n.map(_.bytes).sum + s.map(_.bytes).sum, s, n)
        QuotaUsage(storage)
      }
    case _ => throw new IllegalArgumentException
  }

}
