//package Optimizer
//
//import Explorer.Types.AttributeName
//import Explorer._
//
///**
//  * Initial value population, applies basic type guess information to each attribute, computes key and type entropy and creates initial tree data structure for JERoot
//  */
//object Planner {
//
//  // planner builds the initial values and structure, these should be computations that only happen once
//
//  def buildOperatorTree(jeRoot: JsonExtractionRoot): scala.collection.mutable.ListBuffer[(AttributeName,Double)] = {
//    val kse_intervals: scala.collection.mutable.ListBuffer[(AttributeName,Double)] = scala.collection.mutable.ListBuffer[(AttributeName,Double)]()
//    jeRoot.AllAttributes.foreach{case(name,attribute) => {
//      val kse: Double = keySpaceEntropy(attribute.types)
//      val te: Double = typeEntropy(attribute.types)
//      attribute.keySpaceEntropy = Some(kse)
//      attribute.typeEntropy = Some(te)
//      kse_intervals += Tuple2(name,kse)
//      // now need to make operator tree
//      name.foldLeft(jeRoot.Tree){case(tree,n) => {
//
//        tree.get(n) match {
//          case Some(opNode) =>
//            opNode match {
//              case Some(nod) =>
//                tree.get(n).get.get
//              case None =>
//                tree.put(n,Some(new node()))
//                tree.get(n).get.get
//            }
//          case None =>
//            tree.put(n,Some(new node()))
//            tree.get(n).get.get
//        }
//      }}
//
//    }}
//    return kse_intervals
//  }
//
//
//  // updates all attributes in root's naive types
//  def setNaiveTypes(jeRoot: JsonExtractionRoot): Unit = {
//    jeRoot.AllAttributes.foreach(x=> {
//      x._2.naiveType = typeSummary(x._2.types)
//    })
//  }
//
//  private def typeSummary(attributeTypes: scala.collection.mutable.HashMap[JsonExplorerType,Int]): JsonExplorerType = {
//    var (isObject,isArray) = attributeTypes.foldLeft((true,true)){case((isObject,isArray),(typ,count)) => {
//      typ.getType() match {
//        case JE_Empty_Object | JE_Object => (isObject,false)
//        case JE_Empty_Array | JE_Array => (false,isArray)
//        case JE_Null => (isObject,isArray)
//        case _ => (false,false)
//      }
//    }}
//    isObject = isObject && (attributeTypes.map(_._1.getType()).toList.contains(JE_Empty_Object)||attributeTypes.map(_._1.getType()).toList.contains(JE_Object))
//    isArray = isArray && (attributeTypes.map(_._1.getType()).toList.contains(JE_Empty_Array)||attributeTypes.map(_._1.getType()).toList.contains(JE_Array))
//    if(isObject) {
//      return JE_Object
//    }
//    else if(isArray) {
//      return JE_Array
//    }
//    else {
//      return JE_Basic
//    }
//  }
//
//}
