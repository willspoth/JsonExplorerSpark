package Explorer

import Explorer.Types.AttributeName

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/** Our type conversion, a JSON row can be converted into a JsonExplorerType (JET). The outer record with be a JE_Object that can be recursively traversed to recover all attributes.
  *
  */
sealed trait JsonExplorerType {
  def add(name: String, jet: JsonExplorerType): Unit = ???
  def isEmpty(): Boolean = ???
  def getType(): JsonExplorerType = this match {
    case JE_String => JE_String
    case JE_Numeric => JE_Numeric
    case JE_Boolean => JE_Boolean
    case JE_Null => JE_Null
    case JE_Object => JE_Object
    case _:JE_Object => JE_Object
    case JE_Array => JE_Array
    case _:JE_Array => JE_Array
    case JE_Empty_Object => JE_Empty_Object
    case JE_Empty_Array => JE_Empty_Array
    case JE_Var_Object => JE_Var_Object
    case JE_Var_Object(xs) => JE_Var_Object
    case JE_Obj_Array => JE_Obj_Array
    case JE_Obj_Array(xs) => JE_Obj_Array
    case JE_Tuple => JE_Tuple
  }

  def isBasic(): Boolean = this match {
    case JE_String => true
    case JE_Numeric => true
    case JE_Boolean => true
    case JE_Null => true
    case JE_Object => false
    case _:JE_Object => false
    case JE_Array => false
    case _:JE_Array => false
    case JE_Empty_Object => false
    case JE_Empty_Array => false
    case JE_Var_Object => false
    case JE_Var_Object(xs) => false
    case JE_Obj_Array => false
    case JE_Obj_Array(xs) => false
    case JE_Tuple => false
  }
}

case class FeatureVector(fv: Array[Byte]) {
  val Features: Array[Byte] = fv
  var Count = 1
}

/** Used for efficient traversal of JsonExplorerRoot's attribute list, so structure is preserved
  *
  */
case class node() extends scala.collection.mutable.HashMap[Any,Option[node]] {}

/** Class to express JSON attribute. Created by Extraction and stored by JsonExtractionSchema.
  *
  */
case class Attribute(
                      name: AttributeName = null,
                      `type`: scala.collection.mutable.Set[JsonExplorerType] = null,
                      typeList: scala.collection.mutable.HashMap[JsonExplorerType,Int] = null,
                      objectTypeEntropy: Option[Double] = None,
                      objectMarginalKeySpaceEntropy: Option[Double] = None,
                      objectJointKeySpaceEntropy: Option[Double] = None,
                      arrayTypeEntropy: Option[Double] = None,
                      arrayKeySpaceEntropy: Option[Double] = None,
                      properties: mutable.HashMap[AttributeName,Attribute] = null,
                      items: mutable.ListBuffer[Attribute] = null,
                      required: Boolean = false,
                      multiplicity: Int = 0
                    ) {
}
object Attribute {
  def apply(name: ListBuffer[Any],
            typeList: mutable.HashMap[JsonExplorerType, Int]
           ): Attribute = {

    new Attribute(name,
      Types.getType(typeList),
      typeList,
      Types.objectTypeEntropy(typeList),
      Types.objectMarginalKeySpaceEntropy(typeList),
      Types.objectJointKeySpaceEntropy(typeList),
      Types.arrayTypeEntropy(typeList),
      Types.arrayKeySpaceEntropy(typeList),
      mutable.HashMap[AttributeName,Attribute](),
      mutable.ListBuffer[Attribute]()
    )
  }
}

class AttributeTree(var name: Any, var children: mutable.HashMap[Any,AttributeTree], var attribute: Attribute)


case object JE_String extends JsonExplorerType
case object JE_Numeric extends JsonExplorerType
case object JE_Boolean extends JsonExplorerType
case object JE_Null extends JsonExplorerType
case object JE_Object extends JsonExplorerType
case object JE_Array extends JsonExplorerType
case object JE_Empty_Object extends JsonExplorerType
case object JE_Empty_Array extends JsonExplorerType


case class JE_Array(xs:ListBuffer[JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Array): Option[ListBuffer[JsonExplorerType]] = {
    if(true)
      return Some(arg.asInstanceOf[ListBuffer[JsonExplorerType]])
    else return None
  }

  override def add(name: String, jet: JsonExplorerType): Unit = {xs += jet}
  override def isEmpty(): Boolean = xs.isEmpty
}

case class JE_Object(xs:scala.collection.mutable.HashMap[String,JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Object): Option[scala.collection.mutable.HashMap[String,JE_Object]] = {
    if(true)
      return Some(arg.asInstanceOf[scala.collection.mutable.HashMap[String,JE_Object]])
    else return None
  }
  override def add(name: String, jet: JsonExplorerType): Unit = {
    xs.put(name,jet)
  }

  override def isEmpty(): Boolean = xs.isEmpty
}

// rewritten types after Explorer
case object JE_Tuple extends JsonExplorerType
case object JE_Var_Object extends JsonExplorerType
case class JE_Var_Object(xs:Map[String,JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Var_Object): Option[Map[String,JE_Var_Object]] = {
    if(true)
      return Some(arg.asInstanceOf[Map[String,JE_Var_Object]])
    else return None
  }
}

case object JE_Obj_Array extends JsonExplorerType
case class JE_Obj_Array(xs:ListBuffer[JsonExplorerType]) extends JsonExplorerType {

  def unapply(arg: JE_Obj_Array): Option[ListBuffer[JsonExplorerType]] = {
    if(true)
      return Some(arg.asInstanceOf[ListBuffer[JsonExplorerType]])
    else return None
  }
}



final case class UnknownTypeException(private val message: String = "",
                                      private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

/** Used for extraction type checking.
  *
  */
object ParsingPrimitives {
  val StringClass = classOf[String]
  val DoubleClass = classOf[java.lang.Double]
  val BooleanClass = classOf[java.lang.Boolean]
  val ArrayClass = classOf[java.util.ArrayList[_]]
  val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
  val MapType = new java.util.HashMap[String,Object]().getClass
}

case object Star {
  override def toString: String = "[*]"
}


/** Common functions to preform on JET's and Attributes
  *
  */
object Types {
  /** String friendly conversion, mostly used for visual output like println. Not file naming save, see nameToFileString for that
    *
    * @param name
    */
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

  /** File friendly name conversion
    * @deprecated
    * @param name
    */
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

  /** converts any map of attribute names to tree form with max depth for treeViz planning.
    *
    * @param attributes
    * @return node representation, maximum depth
    */
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

  // Start entropy metrics

  def objectMarginalKeySpaceEntropy(m: scala.collection.mutable.Map[JsonExplorerType,Int]): Option[Double] = {
    val filtered: scala.collection.mutable.Map[JsonExplorerType,Int] = m
      .filter(x => x._1.getType().equals(JE_Object) || x._1.getType().equals(JE_Empty_Object))

    if (filtered.isEmpty)
      return None

    val total: Int = filtered.map(_._2).reduce(_+_)

    val entropyVals: mutable.Iterable[Double] = m
      .foldLeft(mutable.HashMap[String,Int]()){case(attrMap,(jeObj,count)) => {
        jeObj match {
          case JE_Object(xs) => xs.foreach(k => {
            attrMap.get(k._1) match {
              case Some(v) => attrMap.put(k._1,v+count)
              case None => attrMap.put(k._1,count)
            }
          })
          case _ => // do nothing
        }
        attrMap
      }}
      .map(x => (x._2.toDouble / total) * scala.math.log((x._2.toDouble / total)))


    val entropy = if(entropyVals.nonEmpty) entropyVals.reduce(_+_) else 0.0

    if(entropy == 0.0)
      return Some(entropy)
    else
      return Some((-1.0 * entropy))
  }


  def objectJointKeySpaceEntropy(m: scala.collection.mutable.Map[JsonExplorerType,Int]): Option[Double] = {
    val filtered: scala.collection.mutable.Map[JsonExplorerType,Int] = m
      .filter(x => x._1.getType().equals(JE_Object) || x._1.getType().equals(JE_Empty_Object))

    if (filtered.isEmpty)
      return None

    val total: Int = filtered.map(_._2).reduce(_+_)

    val entropyVals: mutable.Iterable[Double] = filtered.map(x => (x._2.toDouble / total) * scala.math.log((x._2.toDouble / total)))

    val entropy = if(entropyVals.nonEmpty) entropyVals.reduce(_+_) else 0.0

    if(entropy == 0.0)
      return Some(entropy)
    else
      return Some((-1.0 * entropy))
  }


  def arrayKeySpaceEntropy(m: scala.collection.mutable.Map[JsonExplorerType,Int]): Option[Double] = {
    val filtered: scala.collection.mutable.Map[JsonExplorerType,Int] = m
      .filter(x => x._1.getType().equals(JE_Array) || x._1.getType().equals(JE_Empty_Array))

    if(filtered.isEmpty)
      return None

    val total: Int = filtered.map(_._2).reduce(_+_)

    val entropy: Double = filtered.map(x => (x._2.toDouble / total) * scala.math.log((x._2.toDouble / total))).reduce(_+_)

    if(entropy == 0.0)
      return Some(entropy)
    else
      return Some((-1.0 * entropy))
  }

  def arrayTypeEntropy(m: scala.collection.mutable.Map[JsonExplorerType,Int]): Option[Double] = {
    val filtered: scala.collection.mutable.Map[JsonExplorerType,Int] = m
      .filter(x => x._1.getType().equals(JE_Array) || x._1.getType().equals(JE_Empty_Array))

    if (filtered.isEmpty)
      return None

    val total: Int = filtered.map(_._2).reduce(_+_)

    val entropy: Double = m
      .foldLeft(mutable.HashMap[JsonExplorerType,Int]()){case(attrMap,(jeArr,count)) => {
        jeArr match {
          case JE_Array(xs) => xs.foreach(k => {
            attrMap.get(k) match {
              case Some(v) => attrMap.put(k,v+count)
              case None => attrMap.put(k,count)
            }
          })
          case JE_Empty_Array =>
            // use JE_Array(ListBuffer()) as a place holder
            attrMap.get(JE_Array(ListBuffer())) match {
              case Some(v) => attrMap.put(JE_Array(ListBuffer()),v+count)
              case None => attrMap.put(JE_Array(ListBuffer()),count)
            }
          case _ => // do nothing
        }
        attrMap
      }}
      .map(x => (x._2.toDouble / total) * scala.math.log((x._2.toDouble / total))).reduce(_+_)

    if(entropy == 0.0)
      return Some(entropy)
    else
      return Some((-1.0 * entropy))
  }

  def objectTypeEntropy(m: scala.collection.mutable.Map[JsonExplorerType,Int]): Option[Double] = {
    val filtered: scala.collection.mutable.Map[JsonExplorerType,Int] = m
      .filter(x => x._1.getType().equals(JE_Object) || x._1.getType().equals(JE_Empty_Object))

    if (filtered.isEmpty)
      return None

    val total: Int = filtered.map(_._2).reduce(_+_)

    val entropy: Double = m
      .foldLeft(mutable.HashMap[JsonExplorerType,Int]()){case(attrMap,(jeObj,count)) => {
        jeObj match {
          case JE_Object(xs) => xs.foreach(k => {
            attrMap.get(k._2) match {
              case Some(v) => attrMap.put(k._2,v+count)
              case None => attrMap.put(k._2,count)
            }
          })
          case JE_Empty_Object =>
            // use JE_Array(ListBuffer()) as a place holder
            attrMap.get(JE_Object(mutable.HashMap[String,JsonExplorerType]())) match {
              case Some(v) => attrMap.put(JE_Object(mutable.HashMap[String,JsonExplorerType]()),v+count)
              case None => attrMap.put(JE_Object(mutable.HashMap[String,JsonExplorerType]()),count)
            }
          case _ => // do nothing
        }
        attrMap
      }}
      .map(x => (x._2.toDouble / total) * scala.math.log((x._2.toDouble / total))).reduce(_+_)

    if(entropy == 0.0)
      return Some(entropy)
    else
      return Some((-1.0 * entropy))
  }

  def typeEntropy(m: scala.collection.mutable.Map[JsonExplorerType,Int]): Double = {

    val entropy: List[(Double,Int)] = m.flatMap{ case(jet,count) => {
      jet match {
        case JE_Array(xs) =>
          val teMap = xs.foldLeft(new scala.collection.mutable.HashMap[JsonExplorerType,Int]()){case(acc,v) =>
            v.getType() match {
              case JE_Empty_Object => acc.update(JE_Object,acc.getOrElseUpdate(JE_Object,0)+1);acc
              case JE_Empty_Array => acc.update(JE_Array,acc.getOrElseUpdate(JE_Array,0)+1);acc
              case _ => acc.update(v,acc.getOrElseUpdate(v,0)+1);acc
            }
          }
          val total = teMap.map(_._2).sum
          val e = teMap.map{case(v,c) => (c/total.toDouble)*math.log(c/total.toDouble)}.sum * -1.0
          List[(Double,Int)](Tuple2(e,count))

        case JE_Object(xs) =>
          val teMap = xs.map(x => x._2).foldLeft(new scala.collection.mutable.HashMap[JsonExplorerType,Int]()){case(acc,v) =>
            v.getType() match {
              case JE_Empty_Object => acc.update(JE_Object,acc.getOrElseUpdate(JE_Object,0)+1);acc
              case JE_Empty_Array => acc.update(JE_Array,acc.getOrElseUpdate(JE_Array,0)+1);acc
              case _ => acc.update(v,acc.getOrElseUpdate(v,0)+1);acc
            }
          }
          val total = teMap.map(_._2).sum
          val e = teMap.map{case(v,c) => (c/total.toDouble)*math.log(c/total.toDouble)}.sum * -1.0
          List[(Double,Int)](Tuple2(e,count))

        case _ => List[(Double,Int)]()
      }
    }}.toList
    val total = entropy.map(_._2).sum
    val e = entropy.map{case(v,c) => v*c/total.toDouble}.sum

    return e
  }

  def getType(m:scala.collection.mutable.Map[JsonExplorerType,Int]): scala.collection.mutable.Set[JsonExplorerType] = {

    val typeSet: mutable.Set[JsonExplorerType] = m.map{case(k,v) => {k.getType()}}.toSet.to[mutable.Set]

    if(typeSet.contains(JE_Array) && typeSet.contains(JE_Empty_Array))
      typeSet.remove(JE_Empty_Array)
    if(typeSet.contains(JE_Object) && typeSet.contains(JE_Empty_Object))
      typeSet.remove(JE_Empty_Object)

    return typeSet
  }

  // using an interval metric to determine a kse breakpoint
  def inferKSE(kse_intervals: scala.collection.mutable.ListBuffer[(scala.collection.mutable.ListBuffer[Any],Double)]): Double = {
    return kse_intervals.map(_._2).foldLeft((0.0,0.0,0.0)){case((largestInter,loc,last),x) => {
      if((x-last) > largestInter)
        ((x-last),last,x)
      else
        (largestInter,loc,x)
    }}._2
  }

  def flattenJET(attribute: JsonExplorerType
                ): mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]] =
  {
    val attributeMap: mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]] = mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]()

    def add(name: AttributeName, jet: JsonExplorerType): Unit =
    {
      attributeMap.get(name) match {
        case Some(a) => a.add(jet)
        case None => attributeMap.put(name,mutable.Set(jet))
      }
    }

    def flatten(name: AttributeName, jet: JsonExplorerType): Unit =
    {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
          add(name,jet.getType())
        case JE_Object(xs) =>
          add(name,jet.getType())
          xs.foreach{case(childName,childJET) => flatten(name++ListBuffer(childName),childJET)}
        case JE_Array(xs) =>
          add(name,jet.getType())
          xs.foreach( childJET => flatten(name++ListBuffer(Star),childJET))
      }
    }

    flatten(ListBuffer[Any](),attribute)
    return attributeMap
  }

  def isStrictSubSet(base: AttributeName, question: AttributeName): Boolean = {
    if(question.isEmpty) // root case
      return true
    if(question.size >= base.size)
      return false
    base.zipAll(question,null,null).map{case(b,q) => (q == null || b.equals(q))}.reduce(_&&_)
  }

  /** ListBuffer[Any] to store attribute names, used to avoid potential escaping and danger characters. Integers mean array value, string is object and read left to right similar to dot notation
    */
  type AttributeName = scala.collection.mutable.ListBuffer[Any]


  case class BiMaxNode(schema: Set[AttributeName], types: Map[AttributeName,mutable.Set[JsonExplorerType]], multiplicity: Int, subsets: mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int)])
  type BiMaxStruct = mutable.Seq[BiMaxNode]
  type DisjointNodes = mutable.Seq[BiMaxStruct]
}