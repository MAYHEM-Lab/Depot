package wtf.knc.depot.model

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}

case class Segment(
  id: Long,
  datasetId: Long,
  version: Long,
  state: SegmentState,
  createdAt: Long,
  updatedAt: Long
)

case class SegmentData(
  path: String,
  checksum: String,
  size: Long,
  rows: Long,
  sample: Seq[Seq[String]]
)

case class SegmentInput(
  targetSegmentId: Long,
  sourceSegmentId: Long,
  binding: String
)

@JsonSerialize(`using` = classOf[StateSerializer])
@JsonDeserialize(`using` = classOf[StateDeserializer])
sealed abstract class SegmentState(val name: String)
object SegmentState {

  def parse(str: String): SegmentState = str match {
    case "Initializing" => SegmentState.Initializing
    case "Announced" => SegmentState.Announced
    case "Awaiting" => SegmentState.Awaiting
    case "Queued" => SegmentState.Queued
    case "Transforming" => SegmentState.Transforming
    case "Materialized" => SegmentState.Materialized
    case "Failed" => SegmentState.Failed
    case "Released" => SegmentState.Released
  }

  case object Initializing extends SegmentState("Initializing")
  case object Announced extends SegmentState("Announced")
  case object Awaiting extends SegmentState("Awaiting")
  case object Queued extends SegmentState("Queued")
  case object Transforming extends SegmentState("Transforming")
  case object Materialized extends SegmentState("Materialized")
  case object Failed extends SegmentState("Failed")
  case object Released extends SegmentState("Released")
}

class StateSerializer extends StdSerializer[SegmentState](classOf[SegmentState]) {
  override def serialize(value: SegmentState, gen: JsonGenerator, provider: SerializerProvider): Unit =
    gen.writeString(value.name)
}

class StateDeserializer extends StdDeserializer[SegmentState](classOf[SegmentState]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): SegmentState =
    SegmentState.parse(p.getText)
}
