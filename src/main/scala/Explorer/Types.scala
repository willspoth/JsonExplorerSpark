package Explorer


sealed trait JsonExplorerType {
  def getType():JsonExplorerType
  def id():Int
}

case class FeatureVector(schema: JsonExtractionSchema) {
  val parentName: scala.collection.mutable.ListBuffer[Any] = schema.parent
  val Features: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Option[JsonExplorerType]] = schema.attributes.map{case(n,a) => (n, None)}
  def updateFeature(n:scala.collection.mutable.ListBuffer[Any],t:JsonExplorerType): Unit = Features.put(n,Some(t))
}

case class node() extends scala.collection.mutable.HashMap[Any,Option[node]] {}

// used in the extraction phase, this holds information of every type
case class Attribute() {
  var name: scala.collection.mutable.ListBuffer[Any] = null
  var naiveType: JsonExplorerType = JE_Null
  var types: scala.collection.mutable.HashMap[JsonExplorerType,Int] = scala.collection.mutable.HashMap[JsonExplorerType,Int]()
  var typeEntropy: Option[Double] = None
  var keySpaceEntropy: Option[Double] = None
}


case class JsonExtractionRoot() {
  val AllAttributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute] = scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute]()
  var localAttributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute] = scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute]()
  // None is a leaf
  var computationTree: node = new node() // tree used for local computations and recursive calls
  val GrandTree: node = new node() // the main tree that expresses all objects and should not change
  val schemas: scala.collection.mutable.ListBuffer[JsonExtractionSchema] = scala.collection.mutable.ListBuffer[JsonExtractionSchema]()
}

case class JsonExtractionSchema() {
  val attributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute] = scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute]()
  // None is a leaf
  var tree: node = null
  var naiveType: JsonExplorerType = null
  var parent: scala.collection.mutable.ListBuffer[Any] = null
}


case object JE_Null extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Null
  def id: Int = 0
}
case object JE_String extends JsonExplorerType {
  def getType: JsonExplorerType = JE_String
  def id: Int = 1
}
case object JE_Numeric extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Numeric
  def id: Int = 2
}
case object JE_Boolean extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Boolean
  def id: Int = 3
}
case object JE_Array extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Array
  def id: Int = 4
}
case class JE_Array(xs:List[JsonExplorerType]) extends JsonExplorerType {
  def apply[A](xs: A*): List[A] = xs.toList

  def unapply(arg: JE_Array): Option[List[JsonExplorerType]] = {
    if(true)
      return Some(arg.asInstanceOf[List[JsonExplorerType]])
    else return None
  }

  def getType: JsonExplorerType = JE_Array
  def id: Int = 4
}
case object JE_Empty_Array extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Empty_Array
  def id: Int = 6
}
case object JE_Object extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Object
  def id: Int = 5
}
case class JE_Object(xs:Map[String,JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Object): Option[Map[String,JE_Object]] = {
    if(true)
      return Some(arg.asInstanceOf[Map[String,JE_Object]])
    else return None
  }
  def getType: JsonExplorerType = JE_Object
  def id: Int = 5
}
case object JE_Empty_Object extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Empty_Object
  def id: Int = 7
}
case object JE_Tuple extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Tuple
  def id: Int = 8
}
case object JE_Unknown extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Unknown
  def id: Int = 9
}
// rewritten types after Explorer
case object JE_Var_Object extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Var_Object
  def id: Int = 11
}
case class JE_Var_Object(xs:Map[String,JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Var_Object): Option[Map[String,JE_Var_Object]] = {
    if(true)
      return Some(arg.asInstanceOf[Map[String,JE_Var_Object]])
    else return None
  }
  def getType: JsonExplorerType = JE_Var_Object
  def id: Int = 11
}
case object JE_Obj_Array extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Obj_Array
  def id: Int = 10
}
case class JE_Obj_Array(xs:List[JsonExplorerType]) extends JsonExplorerType {
  def apply[A](xs: A*): List[A] = xs.toList

  def unapply(arg: JE_Obj_Array): Option[List[JsonExplorerType]] = {
    if(true)
      return Some(arg.asInstanceOf[List[JsonExplorerType]])
    else return None
  }

  def getType: JsonExplorerType = JE_Obj_Array
  def id: Int = 10

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

case object Star