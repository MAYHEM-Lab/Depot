package wtf.knc.depot.model

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import com.twitter.util.Duration

case class Dataset(
  id: Long,
  ownerId: Long,
  tag: String,
  description: String,
  origin: Origin,
  datatype: Datatype,
  visibility: Visibility,
  retention: Option[Duration],
  schedule: Option[Duration],
  clusterAffinity: Option[Long],
  createdAt: Long,
  updatedAt: Long
)

case class DatasetCollaborator(
  datasetId: Long,
  entityId: Long,
  role: Role
)

@JsonSerialize(`using` = classOf[OriginSerializer])
@JsonDeserialize(`using` = classOf[OriginDeserializer])
sealed abstract class Origin(val name: String)
object Origin {
  def parse(str: String): Origin = str match {
    case "Unmanaged" => Unmanaged
    case "Managed" => Managed
  }
  case object Unmanaged extends Origin("Unmanaged")
  case object Managed extends Origin("Managed")
}

class OriginSerializer extends StdSerializer[Origin](classOf[Origin]) {
  override def serialize(value: Origin, gen: JsonGenerator, provider: SerializerProvider): Unit =
    gen.writeString(value.name)
}

class OriginDeserializer extends StdDeserializer[Origin](classOf[Origin]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Origin =
    Origin.parse(p.getText)
}

@JsonSerialize(`using` = classOf[VisibilitySerializer])
@JsonDeserialize(`using` = classOf[VisibilityDeserializer])
sealed abstract class Visibility(val name: String)
object Visibility {
  def parse(str: String): Visibility = str match {
    case "Public" => Public
    case "Private" => Private
  }
  case object Public extends Visibility("Public")
  case object Private extends Visibility("Private")
}

class VisibilitySerializer extends StdSerializer[Visibility](classOf[Visibility]) {
  override def serialize(value: Visibility, gen: JsonGenerator, provider: SerializerProvider): Unit =
    gen.writeString(value.name)
}

class VisibilityDeserializer extends StdDeserializer[Visibility](classOf[Visibility]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Visibility =
    Visibility.parse(p.getText)
}
