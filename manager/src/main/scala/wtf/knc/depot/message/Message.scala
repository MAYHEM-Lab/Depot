package wtf.knc.depot.message

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import wtf.knc.depot.model.Transition

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[Message.SegmentTransition], name = "SegmentTransition"),
    new JsonSubTypes.Type(value = classOf[Message.DatasetSchedule], name = "DatasetSchedule")
  )
)
sealed trait Message
object Message {
  case class SegmentTransition(segmentId: Long, transition: Transition) extends Message
  case class DatasetSchedule(datasetId: Long) extends Message
}
