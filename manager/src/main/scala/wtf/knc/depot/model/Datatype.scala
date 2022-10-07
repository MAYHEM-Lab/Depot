package wtf.knc.depot.model

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import wtf.knc.depot.model.Datatype.Table.ColumnType

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[Datatype.Raw], name = "Raw"),
    new JsonSubTypes.Type(value = classOf[Datatype.Table], name = "Table")
  )
)
sealed trait Datatype
object Datatype {
  case class Raw() extends Datatype

  case class Table(columns: Seq[Table.Column]) extends Datatype
  object Table {

    @JsonSerialize(`using` = classOf[ColumnSerializer])
    @JsonDeserialize(`using` = classOf[ColumnDeserializer])
    sealed abstract class ColumnType(val name: String)
    object ColumnType {
      def parse(str: String): ColumnType = str match {
        case "String" => ColumnType.String
        case "Integer" => ColumnType.Integer
        case "Long" => ColumnType.Long
        case "Double" => ColumnType.Double
        case "Float" => ColumnType.Float
      }

      case object String extends ColumnType("String")
      case object Integer extends ColumnType("Integer")
      case object Long extends ColumnType("Long")
      case object Double extends ColumnType("Double")
      case object Float extends ColumnType("Float")
    }

    case class Column(name: String, columnType: ColumnType)

  }
}

class ColumnSerializer extends StdSerializer[ColumnType](classOf[ColumnType]) {
  override def serialize(value: ColumnType, gen: JsonGenerator, provider: SerializerProvider): Unit =
    gen.writeString(value.name)
}

class ColumnDeserializer extends StdDeserializer[ColumnType](classOf[ColumnType]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): ColumnType =
    ColumnType.parse(p.getText)
}
