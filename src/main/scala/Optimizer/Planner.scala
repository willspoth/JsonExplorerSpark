package Optimizer

import Explorer._

import scala.collection.mutable.ListBuffer

object Planner {

  def buildOperatorTree(jeRoot: JsonExtractionRoot): scala.collection.mutable.ListBuffer[(scala.collection.mutable.ListBuffer[Any],Double)] = {
    val kse_intervals: scala.collection.mutable.ListBuffer[(scala.collection.mutable.ListBuffer[Any],Double)] = scala.collection.mutable.ListBuffer[(scala.collection.mutable.ListBuffer[Any],Double)]()
    jeRoot.AllAttributes.foreach{case(name,attribute) => {
      val kse: Double = keySpaceEntropy(attribute.types)
      attribute.keySpaceEntropy = Some(kse)
      kse_intervals += Tuple2(name,kse)
      // now need to make operator tree
      name.foldLeft(jeRoot.Tree){case(tree,n) => {

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
    return kse_intervals
  }

  private def keySpaceEntropy(m:scala.collection.mutable.Map[JsonExplorerType,Int]): Double = {
    val total: Int = m.foldLeft(0){(count,x) => {
      x._1 match {
        case JE_Empty_Array | JE_Empty_Object => count
        case _ => count + x._2
      }
    }}
    val entropy: Double = m.foldLeft(0.0){(ent,x) => {
      x._1 match {
        case JE_Empty_Array | JE_Empty_Object => ent
        case _ =>
          val p: Double = x._2.toDouble / total
          ent + (p * scala.math.log(p))
      }
    }}
    if(entropy == 0.0)
      return entropy
    else
      return -1.0 * entropy
  }

  /*
  def typeEntropy(typeObject: JsonExplorerType): Double = {




    val m: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map[String,Int]()
    unpackedTypeList.split(",").map(_.split(":")(1)).foreach(x => {
      m.get(x) match {
        case Some(c) => m.update(x,c+1)
        case None => m.update(x,1)
      }
    })
    val total: Int = m.foldLeft(0){(count,x) => count + x._2}
    val entropy: Double = m.foldLeft(0.0){(ent,x) => {
      val p: Double = x._2.toDouble/total
      ent + (p*scala.math.log(p))
    }}
    if(entropy == 0.0)
      return entropy
    else
      return -1.0 * entropy
  }
*/


  def inferKSE(kse_intervals: scala.collection.mutable.ListBuffer[(scala.collection.mutable.ListBuffer[Any],Double)]): Double = {
    return kse_intervals.map(_._2).foldLeft((0.0,0.0,0.0)){case((largestInter,loc,last),x) => {
      if((x-last) > largestInter)
        ((x-last),last,x)
      else
        (largestInter,loc,x)
    }}._2
  }


  // updates all attributes in root's naive types
  def setNaiveTypes(jeRoot: JsonExtractionRoot): Unit = {
    jeRoot.AllAttributes.foreach(x=> {
      x._2.naiveType = typeSummary(x._2.types)})
  }

  private def typeSummary(attributeTypes: scala.collection.mutable.HashMap[JsonExplorerType,Int]): JsonExplorerType = {
    var (isObject,isArray) = attributeTypes.foldLeft((true,true)){case((isObject,isArray),(typ,count)) => {
      typ.getType() match {
        case JE_Empty_Object | JE_Object => (isObject,false)
        case JE_Empty_Array | JE_Array => (false,isArray)
        case JE_Null => (isObject,isArray)
        case _ => (false,false)
      }
    }}
    isObject = isObject && (attributeTypes.contains(JE_Empty_Object)||attributeTypes.contains(JE_Object))
    isArray = isArray && (attributeTypes.contains(JE_Empty_Array)||attributeTypes.contains(JE_Array))
    if(isObject)
      return JE_Array
    else if(isArray)
      return JE_Array
    else
      return JE_Basic
  }

}
