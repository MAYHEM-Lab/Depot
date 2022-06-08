package wtf.knc.depot.model

case class GraphEdge(
  targetDatasetId: Long,
  sourceDatasetId: Long,
  binding: String,
  inputMode: InputMode,
  valid: Boolean,
  createdAt: Long,
  updatedAt: Long
)

sealed trait InputMode
object InputMode {
  case object Ancilla extends InputMode
  case object Trigger extends InputMode
}
