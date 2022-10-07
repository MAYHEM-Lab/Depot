package wtf.knc.depot.model

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

/*
Two modes of retention-class:

A: Strong - each newly announced segment forces the dataset's owner to hold

B: Weak  -
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[RetentionCause.Manual], name = "Manual"),
    new JsonSubTypes.Type(value = classOf[RetentionCause.Dependency], name = "Dependency"),
    new JsonSubTypes.Type(value = classOf[RetentionCause.TTL], name = "TTL")
  )
)
sealed trait RetentionCause
object RetentionCause {
  case class Manual(entityName: String) extends RetentionCause
  case class Dependency(ownerName: String, datasetName: String, segmentVersion: Long) extends RetentionCause
  case class TTL(createdAt: Long) extends RetentionCause
}

// Segment `segmentId` is held by entity `holder` with cause `cause`
case class Retainer(segmentId: Long, holder: Long, state: SegmentState, shallowSize: Long, cause: RetentionCause)
