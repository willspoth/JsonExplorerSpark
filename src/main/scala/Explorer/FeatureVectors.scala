package Explorer

import Explorer.Types.AttributeName

import scala.collection.mutable

object FeatureVectors {

  // TODO this should be a tree :/
  private def getSchema(schemas: Seq[AttributeName], path: AttributeName): AttributeName = {
    schemas.foreach(q => {
      if(Types.isStrictSubSet(path,q))
        return q
    })
    return mutable.ListBuffer[Any]()
  }


  def shredJET(schemas: Seq[AttributeName], attribute: JsonExplorerType
                ): mutable.ListBuffer[(AttributeName, mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]])] =
  {
    val attributeList = mutable.ListBuffer[(AttributeName, mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]])]()

    def flatten(currentSchema: AttributeName, currentMap: mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]], name: AttributeName, jet: JsonExplorerType): Unit =
    {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
          currentMap.put(name,mutable.Set(jet.getType()))
        case JE_Object(xs) =>
          currentMap.put(name,mutable.Set(jet.getType()))
          xs.foreach{case(childName,childJET) => {
            val tempMap = if(schemas.contains(name)) mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]() else currentMap
            flatten(if(schemas.contains(name)) name else currentSchema,tempMap,name++mutable.ListBuffer(childName),childJET)
            if(schemas.contains(name)) attributeList.append((name,tempMap))
          }}
        case JE_Array(xs) =>
          currentMap.put(name,mutable.Set(jet.getType()))
          xs.foreach( childJET => {
            val tempMap = if(schemas.contains(name)) mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]() else currentMap
            flatten(if(schemas.contains(name)) name else currentSchema,tempMap,name++mutable.ListBuffer(Star),childJET)
            if(schemas.contains(name)) attributeList.append((name,tempMap))
          })
      }
    }

    val m = mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]()
    flatten(mutable.ListBuffer[Any](),m,mutable.ListBuffer[Any](),attribute)
    attributeList.append((mutable.ListBuffer[Any](),m))

    return attributeList
  }



  def create(schemas: Seq[AttributeName], row: JsonExplorerType
            ): mutable.ListBuffer[(AttributeName, (AttributeName,mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]))] = // schemaName, schemaName -> repeated for combineByKey
  {
    shredJET(schemas,row).map(x => (x._1,(x._1,x._2)))
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