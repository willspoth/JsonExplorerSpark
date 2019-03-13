package Optimizer

import Explorer.Types.AttributeName
import Explorer._

/**
  * Initial value population, applies basic type guess information to each attribute, computes key and type entropy and creates initial tree data structure for JERoot
  */
object Planner {

  // planner builds the initial values and structure, these should be computations that only happen once

  def buildOperatorTree(jeRoot: JsonExtractionRoot): scala.collection.mutable.ListBuffer[(AttributeName,Double)] = {
    val kse_intervals: scala.collection.mutable.ListBuffer[(AttributeName,Double)] = scala.collection.mutable.ListBuffer[(AttributeName,Double)]()
    jeRoot.AllAttributes.foreach{case(name,attribute) => {
      val kse: Double = keySpaceEntropy(attribute.types)
      val te: Double = typeEntropy(attribute.types)
      attribute.keySpaceEntropy = Some(kse)
      attribute.typeEntropy = Some(te)
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
      x._1.getType() match {
        case JE_Empty_Array | JE_Empty_Object => count
        case _ => count + x._2
      }
    }}
    val entropy: Double = m.foldLeft(0.0){(ent,x) => {
      x._1.getType() match {
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


  def typeEntropy(m:scala.collection.mutable.Map[JsonExplorerType,Int]): Double = {

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
      x._2.naiveType = typeSummary(x._2.types)
    })
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
    isObject = isObject && (attributeTypes.map(_._1.getType()).toList.contains(JE_Empty_Object)||attributeTypes.map(_._1.getType()).toList.contains(JE_Object))
    isArray = isArray && (attributeTypes.map(_._1.getType()).toList.contains(JE_Empty_Array)||attributeTypes.map(_._1.getType()).toList.contains(JE_Array))
    if(isObject) {
      return JE_Object
    }
    else if(isArray) {
      return JE_Array
    }
    else {
      return JE_Basic
    }
  }

}
