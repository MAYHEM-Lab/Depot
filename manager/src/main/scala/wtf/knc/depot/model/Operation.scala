package wtf.knc.depot.model

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[Operation.Notebook], name = "Notebook")
  )
)
sealed trait Operation
object Operation {
  case class Notebook(notebookId: String) extends Operation
}
