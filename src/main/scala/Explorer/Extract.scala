package Explorer

import Explorer.Types.AttributeName

import scala.collection.mutable.ListBuffer

/** Used with flatMap and combineByKey to output all possible rows in JET form with the number of times it occurred
  *
  */

//TODO doesn't need to copy type into new structure
object Extract {
  def ExtractAttributes(row: JsonExplorerType): ListBuffer[(AttributeName,JsonExplorerType)] = {
    val flatMap: ListBuffer[(AttributeName,JsonExplorerType)] = ListBuffer[(AttributeName,JsonExplorerType)]()

    // extracts the types and child types and adds them to root
    def extract(name: AttributeName,jet: JsonExplorerType): Unit = {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object => flatMap += Tuple2(name,jet)
        case JE_Object(xs) =>
          if(name.nonEmpty) {
            flatMap += Tuple2(name,JE_Object(xs.map(je => {
              (je._1, je._2.getType())
            })))
          }
          xs.foreach(je => extract(name :+ je._1, je._2))
        case JE_Array(xs) =>
          if(name.nonEmpty) {
            flatMap += Tuple2(name,JE_Array(xs.map(je => {je.getType()})))
          }
          xs.zipWithIndex.foreach(je => {
            extract(name :+ je._2, je._1)
          })
      }
    }

    extract(new AttributeName(),row)

    flatMap
  }

  def createCombiner(jet: JsonExplorerType): scala.collection.mutable.HashMap[JsonExplorerType,Int] = {
    scala.collection.mutable.HashMap[JsonExplorerType,Int](jet->1)
  }

  def mergeValue(m: scala.collection.mutable.HashMap[JsonExplorerType,Int], jet: JsonExplorerType): scala.collection.mutable.HashMap[JsonExplorerType,Int] = {
    m.get(jet) match {
      case Some(count) => m.update(jet,count+1)
      case None => m.put(jet,1)
    }
    m
  }

  def mergeCombiners(c1: scala.collection.mutable.HashMap[JsonExplorerType,Int], c2: scala.collection.mutable.HashMap[JsonExplorerType,Int]): scala.collection.mutable.HashMap[JsonExplorerType,Int] = {
    if(c1.size >= c2.size){
      c2.foreach{case(n,t) => {
        c1.get(n) match {
          case Some(v) => c1.update(n,v+t)
          case None => c1.put(n,t)
        }
      }}
      c1
    } else {
      c1.foreach{case(n,t) => {
        c2.get(n) match {
          case Some(v) => c2.update(n,v+t)
          case None => c2.put(n,t)
        }
      }}
      c2
    }
  }

}
