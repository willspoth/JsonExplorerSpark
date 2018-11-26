package Explorer



sealed trait JsonExplorerType {
  def getType():JsonExplorerType

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


// used in the extraction phase, this holds information of every type
case class Attribute() {
  var name: scala.collection.mutable.ListBuffer[Any] = null
  val types: scala.collection.mutable.HashMap[JsonExplorerType,Int] = scala.collection.mutable.HashMap[JsonExplorerType,Int]()
  var typeEntropy: Option[Double] = None
  var keySpaceEntropy: Option[Double] = None
}

case class JsonExtractionRoot() {
  val attributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute] = scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute]()
}

case object JE_Null extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Null
}
case object JE_String extends JsonExplorerType {
  def getType: JsonExplorerType = JE_String
}
case object JE_Numeric extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Numeric
}
case object JE_Boolean extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Boolean
}
case object JE_Array extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Array
}
case class JE_Array(xs:List[JsonExplorerType]) extends JsonExplorerType {
  def apply[A](xs: A*): List[A] = xs.toList

  def unapply(arg: JE_Array): Option[List[JsonExplorerType]] = {
    if(true)
      return Some(arg.asInstanceOf[List[JsonExplorerType]])
    else return None
  }

  def getType: JsonExplorerType = JE_Array

}
case object JE_Empty_Array extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Empty_Array
}
case object JE_Object extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Object
}
case class JE_Object(xs:Map[String,JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Object): Option[Map[String,JE_Object]] = {
    if(true)
      return Some(arg.asInstanceOf[Map[String,JE_Object]])
    else return None
  }
  def getType: JsonExplorerType = JE_Object

}
case object JE_Empty_Object extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Empty_Object
}
case object JE_Tuple extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Tuple
}
case object JE_Unknown extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Unknown
}
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
