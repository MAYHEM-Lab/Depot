package wtf.knc.depot.model

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}

case class Cluster(
  id: Long,
  ownerId: Long,
  status: ClusterStatus,
  tag: String,
  createdAt: Long,
  updatedAt: Long
)

case class SparkInfo(
  sparkMaster: String
)

case class TransformerInfo(
  transformer: String
)

case class ConsumerInfo(
                            consumer: String
                          )

case class NotebookInfo(
  notebookMaster: String
)

@JsonSerialize(`using` = classOf[StatusSerializer])
@JsonDeserialize(`using` = classOf[StatusDeserializer])
sealed abstract class ClusterStatus(val name: String)
object ClusterStatus {
  def parse(str: String): ClusterStatus = str match {
    case "Provisioning" => Provisioning
    case "Active" => Active
  }
  case object Provisioning extends ClusterStatus("Provisioning")
  case object Active extends ClusterStatus("Active")
}

class StatusSerializer extends StdSerializer[ClusterStatus](classOf[ClusterStatus]) {
  override def serialize(value: ClusterStatus, gen: JsonGenerator, provider: SerializerProvider): Unit =
    gen.writeString(value.name)
}

class StatusDeserializer extends StdDeserializer[ClusterStatus](classOf[ClusterStatus]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): ClusterStatus =
    ClusterStatus.parse(p.getText)
}
