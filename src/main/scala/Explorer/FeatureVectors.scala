package Explorer

import Explorer.Types.AttributeName

import scala.collection.mutable

object FeatureVectors {

  private def getSchema(schemas: Seq[AttributeName], path: AttributeName): AttributeName = {
    schemas.foreach(q => {
      if(Types.isStrictSubSet(path,q))
        return q
    })
    return mutable.ListBuffer[Any]()
  }

  def create(schemas: Seq[AttributeName],row: JsonExplorerType
            ): List[(AttributeName, mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]])] =
  {
    val rows = mutable.HashMap[AttributeName, mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]]()

    Types.flattenJET(row).map(v => {
      val schema = getSchema(schemas,v._1)
      rows.get(schema) match {
        case Some(s) => s.put(v._1,v._2)
        case None => rows.put(schema,mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]](v))
      }
    })
    rows.toList
  }

  def createCombiner(row: mutable.HashMap[AttributeName, mutable.Set[JsonExplorerType]]
                    ): mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int] = // schema attributes
  {
    val m =mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int]()
    m.put(row.toMap,1)
    m
  }

  def mergeValue(comb: mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],
                 row: mutable.HashMap[AttributeName, mutable.Set[JsonExplorerType]]
                ): mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int] = {
    val m = row.toMap
    comb.get(m) match {
      case Some(i) => comb.put(m,i+1)
      case None => comb.put(m,1)
    }
    comb
  }

  def mergeCombiners(c1: mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],
                     c2: mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int]
                    ): mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int] = {
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
