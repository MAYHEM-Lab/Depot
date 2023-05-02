package wtf.knc.depot.model

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[Entity.User], name = "User"),
    new JsonSubTypes.Type(value = classOf[Entity.Organization], name = "Organization")
  )
)
sealed trait Entity {
  val id: Long
  val name: String
}
object Entity {
  val Root: Organization = Organization(1, "root", 0L)

  case class User(id: Long, name: String, createdAt: Long) extends Entity
  case class Organization(id: Long, name: String, createdAt: Long) extends Entity
}

@JsonSerialize(`using` = classOf[RoleSerializer])
@JsonDeserialize(`using` = classOf[RoleDeserializer])
sealed abstract class Role(val name: String)
object Role {
  def parse(str: String): Role = str match {
    case "Owner" => Owner
    case "Member" => Member
  }
  case object Owner extends Role("Owner")
  case object Member extends Role("Member")
}

class RoleSerializer extends StdSerializer[Role](classOf[Role]) {
  override def serialize(value: Role, gen: JsonGenerator, provider: SerializerProvider): Unit =
    gen.writeString(value.name)
}

class RoleDeserializer extends StdDeserializer[Role](classOf[Role]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Role =
    Role.parse(p.getText)
}
