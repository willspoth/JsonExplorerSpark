package Explorer

sealed trait JsonExplorerType {
  def id(t: JsonExplorerType): Int = {
    t match {
      case JE_Null => return 0
      case JE_String => return 1
      case JE_Numeric => return 2
      case JE_Boolean => return 3
      case JE_Array(xs) => return 4
      case JE_Array => return 4
      case JE_Object(xs) => return 5
      case JE_Object => return 5
      case JE_Empty_Array => return 6
      case JE_Empty_Object => return 7
      case JE_Unknown => return 8
    }
  }
}

case object JE_Null extends JsonExplorerType
case object JE_String extends JsonExplorerType
case object JE_Numeric extends JsonExplorerType
case object JE_Boolean extends JsonExplorerType
case object JE_Array extends JsonExplorerType
case class JE_Array(xs:List[JsonExplorerType]) extends JsonExplorerType {
  def apply[A](xs: A*): List[A] = xs.toList

  def unapply(arg: JE_Array): Option[List[JsonExplorerType]] = {
    if(true)
      return Some(arg.asInstanceOf[List[JsonExplorerType]])
    else return None
  }
}
case object JE_Empty_Array extends JsonExplorerType
case object JE_Object extends JsonExplorerType
case class JE_Object(xs:Map[String,JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Object): Option[Map[String,JE_Object]] = {
    if(true)
      return Some(arg.asInstanceOf[Map[String,JE_Object]])
    else return None
  }
}
case object JE_Empty_Object extends JsonExplorerType
case object JE_Tuple extends JsonExplorerType
case object JE_Unknown extends JsonExplorerType
final case class UnknownTypeException(private val message: String = "",
                                      private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object ParsingPrimitives {
  val StringClass = classOf[String]
  val DoubleClass = classOf[java.lang.Double]
  val BooleanClass = classOf[java.lang.Boolean]
  val ArrayClass = classOf[java.util.ArrayList[_]]
  val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
  val MapType = new java.util.HashMap[String,Object]().getClass
}
