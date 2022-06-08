package wtf.knc.depot.model

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[Trigger.Creation], name = "Creation"),
    new JsonSubTypes.Type(value = classOf[Trigger.Scheduled], name = "Scheduled"),
    new JsonSubTypes.Type(value = classOf[Trigger.Manual], name = "Manual"),
    new JsonSubTypes.Type(value = classOf[Trigger.Upstream], name = "Upstream"),
    new JsonSubTypes.Type(value = classOf[Trigger.Downstream], name = "Downstream")
  )
)
sealed trait Trigger
object Trigger {
  case class Creation(datasetId: Long) extends Trigger
  case class Scheduled(at: Long) extends Trigger
  case class Manual(who: Long) extends Trigger
  case class Upstream(segmentId: Long) extends Trigger
  case class Downstream(segmentId: Long) extends Trigger
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[Transition.Fail], name = "Fail"),
    new JsonSubTypes.Type(value = classOf[Transition.Announce], name = "Announce"),
    new JsonSubTypes.Type(value = classOf[Transition.Await], name = "Await"),
    new JsonSubTypes.Type(value = classOf[Transition.Enqueue], name = "Enqueue"),
    new JsonSubTypes.Type(value = classOf[Transition.Transform], name = "Transform"),
    new JsonSubTypes.Type(value = classOf[Transition.Materialize], name = "Materialize")
  )
)
sealed abstract class Transition(val to: SegmentState)
object Transition {
  case class Fail(cause: String, message: String) extends Transition(SegmentState.Failed)
  case class Announce(trigger: Trigger) extends Transition(SegmentState.Announced)
  case class Await(trigger: Trigger) extends Transition(SegmentState.Awaiting)
  case class Enqueue() extends Transition(SegmentState.Queued)
  case class Transform() extends Transition(SegmentState.Transforming)
  case class Materialize(data: SegmentData) extends Transition(SegmentState.Materialized)
}
