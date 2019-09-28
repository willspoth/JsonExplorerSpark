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
            ): List[(AttributeName, (AttributeName,mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]))] =
  {
    val rows = mutable.HashMap[AttributeName, mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]]()

    Types.flattenJET(row).map(v => {
      val schema = getSchema(schemas,v._1)
      rows.get(schema) match {
        case Some(s) => s.put(v._1,v._2)
        case None => rows.put(schema,mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]](v))
      }
    })
    rows.map(x => (x._1,(x._1,x._2))).toList
  }

  def createCombiner(variableObjects: Set[AttributeName],
                     row: (AttributeName,mutable.HashMap[AttributeName, mutable.Set[JsonExplorerType]])
                    ): Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]] = // schema attributes
  {
    if(variableObjects.contains(row._1)) { // var object
      val m = mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]()
      row._2.foreach(x => {
        m.put(x._1,(x._2,1))
      })
      Right(m)
    } else {
      val m = mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int]()
      m.put(row._2.toMap,1)
      Left(m)
    }
  }

  def mergeValue(comb: Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]],
                 row: (AttributeName,mutable.HashMap[AttributeName, mutable.Set[JsonExplorerType]])
                ): Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]] = {

    comb match {
      case Right(r) => // var obj
        row._2.foreach(x => {
          r.get(x._1) match {
            case Some(v) => r.put(x._1,(x._2 ++ v._1,v._2 + 1))
            case None => r.put(x._1,(x._2,1))
          }
        })
        Right(r)
      case Left(l) =>
        val m = row._2.toMap
        l.get(m) match {
          case Some(i) => l.put(m,i+1)
          case None => l.put(m,1)
        }
        Left(l)
    }
  }

  def mergeCombiners(m1: Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]],
                     m2: Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]]
                    ): Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]] = {

    if(m1.isLeft && m2.isLeft){
      val c1 = m1.left.get
      val c2 = m2.left.get
      if(c1.size >= c2.size){
        c2.foreach{case(n,t) => {
          c1.get(n) match {
            case Some(v) => c1.update(n,v+t)
            case None => c1.put(n,t)
          }
        }}
        Left(c1)
      } else {
        c1.foreach{case(n,t) => {
          c2.get(n) match {
            case Some(v) => c2.update(n,v+t)
            case None => c2.put(n,t)
          }
        }}
        Left(c2)
      }
    } else if(m1.isRight && m2.isRight) {
      val c1 = m1.right.get
      val c2 = m2.right.get
      if(c1.size >= c2.size){
        c2.foreach{case(n,t) => {
          c1.get(n) match {
            case Some(v) => c1.update(n,(v._1++t._1,v._2 + t._2))
            case None => c1.put(n,t)
          }
        }}
        Right(c1)
      } else {
        c1.foreach{case(n,t) => {
          c2.get(n) match {
            case Some(v) => c2.update(n,(v._1++t._1,v._2 + t._2))
            case None => c2.put(n,t)
          }
        }}
        Right(c2)
      }
    } else { // Something really bad just happened
      throw new Exception("Combiners not right!")
    }

  }

}