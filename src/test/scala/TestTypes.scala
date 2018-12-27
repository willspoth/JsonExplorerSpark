import Explorer.{JE_Array, JE_String, JsonExplorerType}

import scala.collection.mutable.ListBuffer

object TestTypes {
  def TestTypes() {
    val a: JsonExplorerType = JE_Array(ListBuffer(JE_String))
    a match {
      case JE_String =>
        println("String")
      case JE_Array(xs:ListBuffer[JsonExplorerType]) =>
        println(xs)

    }
  }
}
