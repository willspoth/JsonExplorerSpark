package Explorer

import Explorer.Types.AttributeName

import scala.collection.mutable.ListBuffer


sealed trait JsonExplorerType {
  def getType(): JsonExplorerType
  def id(): Int
  def add(name: String, jet: JsonExplorerType): Unit = ???
  def isEmpty(): Boolean = ???
}

case class FeatureVector(fv: Array[Byte]) {
  val Features: Array[Byte] = fv
  var Count = 1
}


case class node() extends scala.collection.mutable.HashMap[Any,Option[node]] {}

// used in the extraction phase, this holds information of every type
case class Attribute() {
  var name: scala.collection.mutable.ListBuffer[Any] = null
  var naiveType: JsonExplorerType = JE_Null
  var types: scala.collection.mutable.HashMap[JsonExplorerType,Int] = scala.collection.mutable.HashMap[JsonExplorerType,Int]()
  var typeEntropy: Option[Double] = None
  var keySpaceEntropy: Option[Double] = None

  override def clone(): Attribute = {
    val a = new Attribute()
    a.name = name.clone()
    a.naiveType = naiveType.getType()
    a.types = types.clone()
    a.typeEntropy = typeEntropy match {
      case Some(v) => new Some(v)
      case None => None
    }
    a.keySpaceEntropy = keySpaceEntropy match {
      case Some(v) => new Some(v)
      case None => None
    }
    a
  }
}


case class JsonExtractionRoot() {
  var AllAttributes: scala.collection.mutable.HashMap[AttributeName,Attribute] = scala.collection.mutable.HashMap[AttributeName,Attribute]()
  var Tree: node = new node() // the main tree that expresses all objects and should not change
  var Schemas: scala.collection.mutable.HashMap[AttributeName,JsonExtractionSchema] = scala.collection.mutable.HashMap[AttributeName,JsonExtractionSchema]()
}

case class JsonExtractionSchema() {
  val attributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute] = scala.collection.mutable.HashMap[AttributeName,Attribute]()
  var attributeLookup: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Int] = null
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
case class JE_Array(xs:ListBuffer[JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Array): Option[ListBuffer[JsonExplorerType]] = {
    if(true)
      return Some(arg.asInstanceOf[ListBuffer[JsonExplorerType]])
    else return None
  }

  def getType: JsonExplorerType = JE_Array
  def id: Int = 4
  override def add(name: String, jet: JsonExplorerType): Unit = {xs += jet}
  override def isEmpty(): Boolean = xs.isEmpty
}
case object JE_Empty_Array extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Empty_Array
  def id: Int = 6
}
case object JE_Object extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Object
  def id: Int = 5
}
case class JE_Object(xs:scala.collection.mutable.HashMap[String,JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Object): Option[scala.collection.mutable.HashMap[String,JE_Object]] = {
    if(true)
      return Some(arg.asInstanceOf[scala.collection.mutable.HashMap[String,JE_Object]])
    else return None
  }
  def getType: JsonExplorerType = JE_Object
  def id: Int = 5
  override def add(name: String, jet: JsonExplorerType): Unit = {
    xs.put(name,jet)
  }

  override def isEmpty(): Boolean = xs.isEmpty
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
case class JE_Obj_Array(xs:ListBuffer[JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Obj_Array): Option[ListBuffer[JsonExplorerType]] = {
    if(true)
      return Some(arg.asInstanceOf[ListBuffer[JsonExplorerType]])
    else return None
  }

  def getType: JsonExplorerType = JE_Obj_Array
  def id: Int = 10

}
case object JE_Basic extends JsonExplorerType {
  def getType: JsonExplorerType = JE_Basic
  def id: Int = 12
}

case class JE_Basic(xs: scala.collection.mutable.HashMap[JsonExplorerType,Int]) extends JsonExplorerType {
  def unapply(arg: JE_Basic): Option[scala.collection.mutable.HashMap[JsonExplorerType,Int]] = return Some(xs)
  def getType: JsonExplorerType = JE_Basic
  def id: Int = 12
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

object Types {
  def nameToString(name: scala.collection.mutable.ListBuffer[Any]): String = {
    if(name.isEmpty)
      return "root"
    val nameString = name.foldLeft("")((acc,n)=>{
      n match {
        case s:String => acc + s + "."
        case i: Int => acc + s"""[$i]"""
        case Star => acc + s"""[*]"""
      }
    })
    if(nameString.last.equals('.'))
      return nameString.substring(0,nameString.size-1)
    else
      return nameString
  }

  def nameToFileString(name: Types.AttributeName): String = {
    if(name.isEmpty)
      return "root"
    val nameString = name.foldLeft("")((acc,n)=>{
      n match {
        case s:String => acc + s.replace("-","").replace(":","").replace(";","") + "_"
        case i: Int => acc + s"""_${i}_"""
        case Star => acc + s"""_star_"""
      }
    })
    if(nameString.last.equals('_'))
      return nameString.substring(0,nameString.size-1)
    else
      return nameString
  }

  def buildNodeTree(attributes: scala.collection.mutable.HashMap[Types.AttributeName,_]): (node,Int) = {
    val depth: Int = attributes.foldLeft(0){case (maxDepth,n) => Math.max(n._1.size,maxDepth)}+1

    val tree = new node()
    attributes.foreach{case(name,attribute) => {
      // now need to make operator tree
      name.foldLeft(tree){case(tree,n) => {

        tree.get(n) match {
          case Some(opNode) =>
            opNode match {
              case Some(nod) =>
                tree.get(n).get.get
              case None =>
                tree.put(n,Some(new node()))
                tree.get(n).get.get
            }
          case None =>
            tree.put(n,Some(new node()))
            tree.get(n).get.get
        }
      }}

    }}
    return (tree,depth)
  }

  type AttributeName = scala.collection.mutable.ListBuffer[Any]
  type SchemaName = scala.collection.mutable.ListBuffer[Any]
}