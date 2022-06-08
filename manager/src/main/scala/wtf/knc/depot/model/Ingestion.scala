package wtf.knc.depot.model

sealed trait IngestionMode
object IngestionMode {
  case object Manual extends IngestionMode
  case class Scheduled(interval: Long) extends IngestionMode
}

case class Ingestion(
  datasetId: Long,
  mode: IngestionMode,
  createdAt: Long,
  updatedAt: Long
)
