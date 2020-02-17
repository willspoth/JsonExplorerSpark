package Explorer

import Explorer.Types.AttributeName

import scala.collection.mutable.ListBuffer

object FastExtract {
  def ExtractAttributes(row: JsonExplorerType): ListBuffer[(AttributeName,JsonExplorerType)] = {
    val flatMap: ListBuffer[(AttributeName,JsonExplorerType)] = ListBuffer[(AttributeName,JsonExplorerType)]()

    // extracts the types and child types and adds them to root
    def extract(name: AttributeName,jet: JsonExplorerType): Unit = {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object => flatMap += Tuple2(name,jet)
        case x: JE_Object =>
          if(name.nonEmpty) {
            flatMap += Tuple2(name,JE_Object(x.shallowCopy()))
          }
          x.xs.foreach(je => extract(name :+ je._1, je._2))
        case x:JE_Array =>
          if(name.nonEmpty) {
            flatMap += Tuple2(name,JE_Array(x.shallowCopy()))
          }
          x.xs.zipWithIndex.foreach(je => {
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
    m.put(jet,m.getOrElseUpdate(jet,0)+1)
    m
  }

  def mergeCombiners(c1: scala.collection.mutable.HashMap[JsonExplorerType,Int], c2: scala.collection.mutable.HashMap[JsonExplorerType,Int]): scala.collection.mutable.HashMap[JsonExplorerType,Int] = {
    c2.foreach{case(n,t) => {
      c1.get(n) match {
        case Some(v) => c1.update(n,v+t)
        case None => c1.put(n,t)
      }
    }}
    c1
  }

}
