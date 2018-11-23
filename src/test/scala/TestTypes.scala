import Explorer.{JE_Array, JE_String, JsonExplorerType}

object TestTypes {
  def TestTypes() {
    val a: JsonExplorerType = JE_Array(List(JE_String))
    a match {
      case JE_String =>
        println("String")
      case JE_Array(xs:List[JsonExplorerType]) =>
        println(xs)

    }
  }
}
