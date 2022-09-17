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
  storageClass: StorageClass,
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

@JsonSerialize(`using` = classOf[StorageClassSerializer])
@JsonDeserialize(`using` = classOf[StorageClassDeserializer])
sealed abstract class StorageClass(val name: String)
object StorageClass {
  case object Guaranteed extends StorageClass("Guaranteed")
  case object Transient extends StorageClass("Transient")
  def parse(str: String): StorageClass = str match {
    case "Guaranteed" => StorageClass.Guaranteed
    case "Transient" => StorageClass.Transient
  }
}

class StorageClassSerializer extends StdSerializer[StorageClass](classOf[StorageClass]) {
  override def serialize(v: StorageClass, gen: JsonGenerator, p: SerializerProvider): Unit = gen.writeString(v.name)
}

class StorageClassDeserializer extends StdDeserializer[StorageClass](classOf[StorageClass]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): StorageClass = StorageClass.parse(p.getText)
}

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
  override def serialize(v: Origin, gen: JsonGenerator, p: SerializerProvider): Unit = gen.writeString(v.name)
}

class OriginDeserializer extends StdDeserializer[Origin](classOf[Origin]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Origin = Origin.parse(p.getText)
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
  override def serialize(v: Visibility, gen: JsonGenerator, p: SerializerProvider): Unit = gen.writeString(v.name)
}

class VisibilityDeserializer extends StdDeserializer[Visibility](classOf[Visibility]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Visibility = Visibility.parse(p.getText)
}
