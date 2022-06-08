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
