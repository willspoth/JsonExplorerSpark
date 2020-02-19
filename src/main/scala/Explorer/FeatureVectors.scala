package Explorer

import Explorer.Types.AttributeName

import scala.collection.mutable

object FeatureVectors {

  // TODO this should be a tree :/
  private def getSchema(schemas: Set[AttributeName], path: AttributeName): AttributeName = {
    schemas.foreach(q => {
      if(Types.isStrictSubSet(path,q))
        return q
    })
    return mutable.ListBuffer[Any]()
  }


//  def shredJET(varObjs: Set[AttributeName], objArray: Set[AttributeName], attribute: JsonExplorerType
//                ): mutable.ListBuffer[(AttributeName, Map[AttributeName,mutable.Set[JsonExplorerType]])] =
//  {
//    val attributeList = mutable.ListBuffer[(AttributeName, AttributeName, Map[AttributeName,mutable.Set[JsonExplorerType]])]()
//
//    def flatten(currentSchema: AttributeName, name: AttributeName, jet: JsonExplorerType): Map[AttributeName,mutable.Set[JsonExplorerType]] =
//    {
//      jet match {
//        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
//          return Map[AttributeName,mutable.Set[JsonExplorerType]]((name,mutable.Set(jet.getType())))
//        case JE_Object(xs) =>
//          if(varObjs.contains(name) && !name.equals(currentSchema)){ // variable object
//            attributeList.append((name,(name,flatten(name,name,jet))))
//            return Map[AttributeName,mutable.Set[JsonExplorerType]]((name,mutable.Set(jet.getType())))
//          } else {
//            return Map[AttributeName,mutable.Set[JsonExplorerType]]((name,mutable.Set(jet.getType()))) ++
//              xs.map(x => flatten(currentSchema,name++mutable.ListBuffer(x._1),x._2)).reduce(_++_)
//          }
//
//        case JE_Array(xs) =>
//          if(objArray.contains(name) && !name.equals(currentSchema)){ // variable object
//            attributeList.append((name,(name,flatten(name,name,jet))))
//            return Map[AttributeName,mutable.Set[JsonExplorerType]]((name,mutable.Set(jet.getType())))
//          } else {
//            return Map[AttributeName,mutable.Set[JsonExplorerType]]((name,mutable.Set(jet.getType()))) ++
//              xs.map(x => flatten(currentSchema,name++mutable.ListBuffer(Star),x)).reduce(_++_)
//          }
//      }
//    }
//
//    attributeList.append((mutable.ListBuffer[Any](),(mutable.ListBuffer[Any](),flatten(mutable.ListBuffer[Any](),mutable.ListBuffer[Any](),attribute))))
//
//    return attributeList
//  }

  def shredJET(varObjs: Set[AttributeName], objArray: Set[AttributeName], attribute: JsonExplorerType
              ): mutable.ListBuffer[(AttributeName,(AttributeName, mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]))] =
  {
    val attributeList = mutable.ListBuffer[(AttributeName,(AttributeName, mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]))]() // can't just use map because of array of objects

    def flatten(currentSchema: AttributeName, currentMap: mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]], name: AttributeName, jet: JsonExplorerType): Unit =
    {

      currentMap.get(name) match {
        case Some(set) => set.add(jet.getType())
        case None => currentMap.put(name,mutable.Set(jet.getType()))
      }

      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
          //currentMap.put(name,mutable.Set(jet.getType()))
        case JE_Object(xs) =>
          //currentMap.put(name,mutable.Set(jet.getType()))
          if(varObjs.contains(name)){
            val tempMap =  mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]()
            xs.foreach{case(childName,childJET) => {
              flatten(name,tempMap,name++mutable.ListBuffer(childName),childJET)
            }}
            attributeList.append((name,(name,tempMap)))
          } else {
            xs.foreach{case(childName,childJET) => {
              flatten(currentSchema,currentMap,name++mutable.ListBuffer(childName),childJET)
            }}
          }

        case JE_Array(xs) =>
          //currentMap.put(name,mutable.Set(jet.getType()))
          if(objArray.contains(name)){
            xs.foreach{childJET => {
              val tempMap =  mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]()
              flatten(name,tempMap,name++mutable.ListBuffer(Star),childJET)
              attributeList.append((name,(name,tempMap)))
            }}
          } else {
            xs.foreach{childJET => {
              flatten(currentSchema,currentMap,name++mutable.ListBuffer(Star),childJET)
            }}
          }
      }
    }

    val m = mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]()
    flatten(mutable.ListBuffer[Any](),m,mutable.ListBuffer[Any](),attribute)
    attributeList.append((mutable.ListBuffer[Any](),(mutable.ListBuffer[Any](),m)))

    return attributeList
  }


//  def extractTypes(varObjs: Set[AttributeName], objArray: Set[AttributeName],currentSchema: AttributeName, name: AttributeName, jet: JsonExplorerType,
//                   m: mutable.ListBuffer[(AttributeName,(AttributeName,mutable.Set[JsonExplorerType]))]): Unit =
//  {
//    jet match {
//      case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
//        m.append((currentSchema,(name,mutable.Set(jet.getType()))))
//      case JE_Object(xs) =>
//        m.append((currentSchema,(name,mutable.Set(jet.getType()))))
//        if(varObjs.contains(name)){
//          xs.foreach{case(childName,childJET) => {
//            extractTypes(varObjs, objArray, name ,name++mutable.ListBuffer(childName),childJET,m)
//          }}
//        } else {
//          xs.foreach{case(childName,childJET) => {
//            extractTypes(varObjs, objArray, currentSchema, name++mutable.ListBuffer(childName),childJET, m)
//          }}
//        }
//
//      case JE_Array(xs) =>
//        m.append((currentSchema,(name,mutable.Set(jet.getType()))))
//        if(objArray.contains(name)){
//          xs.zipWithIndex.foreach{case(childJET,idx) => {
//            extractTypes(varObjs, objArray, name++mutable.ListBuffer(idx),name++mutable.ListBuffer(Star),childJET,m)
//          }}
//        } else {
//          xs.foreach{childJET => {
//            extractTypes(varObjs, objArray, currentSchema,name++mutable.ListBuffer(Star),childJET, m)
//          }}
//        }
//    }
//  }
//
//  def shredJET(varObjs: Set[AttributeName], objArray: Set[AttributeName], attribute: JsonExplorerType
//              ): mutable.ListBuffer[(AttributeName,(AttributeName, Map[AttributeName,mutable.Set[JsonExplorerType]]))] =
//  {
//    val m: mutable.ListBuffer[(AttributeName,(AttributeName,mutable.Set[JsonExplorerType]))] = mutable.ListBuffer[(AttributeName,(AttributeName,mutable.Set[JsonExplorerType]))]()
//
//    extractTypes(varObjs, objArray, mutable.ListBuffer[Any](),mutable.ListBuffer[Any](),attribute, m)
//
//    return m.groupBy(_._1).map(x => (x._1,(x._1,x._2.map(_._2).toMap)))
//  }



  def createCombiner(varObjs: Set[AttributeName],
                     objArr: Set[AttributeName],
                     row: (AttributeName,mutable.HashMap[AttributeName, mutable.Set[JsonExplorerType]])
                    ): Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]] = // schema attributes
  {
    if(varObjs.contains(row._1)) { // var object
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