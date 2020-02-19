package Explorer

import Explorer.Types.AttributeName
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object FastFeatureVector {

//    def shredJET(varObjs: Set[AttributeName], objArray: Set[AttributeName], attribute: JsonExplorerType
//                  ): mutable.ListBuffer[(AttributeName, Map[AttributeName,JsonExplorerType])] =
//    {
//      val attributeList = mutable.ListBuffer[(AttributeName, Map[AttributeName,JsonExplorerType])]()
//
//      def flatten(currentSchema: AttributeName, name: AttributeName, jet: JsonExplorerType): Map[AttributeName,JsonExplorerType] =
//      {
//        jet match {
//          case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
//            return Map[AttributeName,JsonExplorerType]((name->jet.getType()))
//          case JE_Object(xs) =>
//            if(varObjs.contains(name) && !name.equals(currentSchema)){ // variable object
//              attributeList.append((name,flatten(name,name,jet)))
//              return Map[AttributeName,JsonExplorerType]((name -> jet.getType()))
//            } else {
//              return Map[AttributeName,JsonExplorerType]((name,jet.getType())) ++
//                xs.map(x => flatten(currentSchema,name++mutable.ListBuffer(x._1),x._2)).reduce(_++_)
//            }
//
//          case JE_Array(xs) =>
//            if(objArray.contains(name) && !name.equals(currentSchema)){ // array of objects
//              attributeList.append((name,flatten(name,name,jet)))
//              return Map[AttributeName,JsonExplorerType]((name,jet.getType()))
//            } else {
//              return Map[AttributeName,JsonExplorerType]((name,jet.getType())) ++
//                xs.map(x => flatten(currentSchema,name++mutable.ListBuffer(Star),x)).reduce(_++_) // return list of maps
//            }
//        }
//      }
//
//      attributeList.append((mutable.ListBuffer[Any](),flatten(mutable.ListBuffer[Any](),mutable.ListBuffer[Any](),attribute)))
//
//      return attributeList
//    }


  def shredJET(varObjs: Set[AttributeName], objArray: Set[AttributeName], attribute: JsonExplorerType
              ): mutable.ListBuffer[((AttributeName, Map[AttributeName,JsonExplorerType]),Int)] =
  {
    val attributeList = mutable.ListBuffer[(AttributeName, mutable.HashMap[AttributeName,JsonExplorerType])]() // can't just use map because of array of objects

    def flatten(currentSchema: AttributeName, currentMap: mutable.HashMap[AttributeName,JsonExplorerType], name: AttributeName, jet: JsonExplorerType): Unit =
    {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
          currentMap.put(name,jet.getType())
        case JE_Object(xs) =>
          currentMap.put(name,jet.getType())
          if(varObjs.contains(name)){
            val tempMap =  mutable.HashMap[AttributeName,JsonExplorerType]()
            xs.foreach{case(childName,childJET) => {
              flatten(name,tempMap,name++mutable.ListBuffer(childName),childJET)
            }}
            attributeList.append((name,tempMap))
          } else {
            xs.foreach{case(childName,childJET) => {
              flatten(currentSchema,currentMap,name++mutable.ListBuffer(childName),childJET)
            }}
          }

        case JE_Array(xs) =>
          currentMap.put(name,jet.getType())
          if(objArray.contains(name)){
            xs.foreach{childJET => {
              val tempMap =  mutable.HashMap[AttributeName,JsonExplorerType]()
              flatten(name,tempMap,name++mutable.ListBuffer(Star),childJET)
              attributeList.append((name,tempMap))
            }}
          } else {
            xs.foreach{childJET => {
              flatten(currentSchema,currentMap,name++mutable.ListBuffer(Star),childJET)
            }}
          }
      }
    }

    val m = mutable.HashMap[AttributeName,JsonExplorerType]()
    flatten(mutable.ListBuffer[Any](),m,mutable.ListBuffer[Any](),attribute)
    attributeList.append((mutable.ListBuffer[Any](),m))

    return attributeList.map(x => ((x._1,x._2.toMap),1))
  }


  def createAttributeCombiner(row: (AttributeName,Map[AttributeName, JsonExplorerType],Int)
                    ): mutable.HashMap[AttributeName, mutable.HashMap[JsonExplorerType,Int]] = {

    val m = mutable.HashMap[AttributeName, mutable.HashMap[JsonExplorerType,Int]]()
    row._2.foreach(x => m.put(x._1,mutable.HashMap[JsonExplorerType,Int]((x._2 -> row._3))))
    return m
  }

  def mergeAttributeValue(acc: mutable.HashMap[AttributeName, mutable.HashMap[JsonExplorerType,Int]],
                          row: (AttributeName,Map[AttributeName, JsonExplorerType],Int)): mutable.HashMap[AttributeName, mutable.HashMap[JsonExplorerType,Int]] = {
    row._2.foreach{case(n,t) => {
      val m = acc.get(n).get
      m.put(t,(m.getOrElse(t,0) + row._3))
    }}
    acc
  }

  def mergeAttributeCombiner(comb1: mutable.HashMap[AttributeName, mutable.HashMap[JsonExplorerType,Int]],
                          comb2: mutable.HashMap[AttributeName, mutable.HashMap[JsonExplorerType,Int]]
                         ): mutable.HashMap[AttributeName, mutable.HashMap[JsonExplorerType,Int]] = {
    comb2.foreach{case(n1,t1) => {
      val m1 = comb1.get(n1).get
      t1.foreach{case(n2,c) => {
        m1.put(n2,(m1.getOrElse(n2,0) + c))
      }}
    }}

    comb1
  }


  def mergeFeatureVectors(fv1: FeatureVector, fv2: FeatureVector): Unit = {
    fv2.typeCount.foreach(x => {
      fv1.typeCount.get(x._1) match {
        case Some(v) =>
          x._2.foreach( y => v.put(y._1,v.getOrElse(y._1,0)+y._2))
        case None => fv1.typeCount.put(x._1,x._2)
      }
    })
  }


  def createCombiner(row: FeatureVector
                    ): mutable.ListBuffer[FeatureVector] = mutable.ListBuffer[FeatureVector](row)

  def mergeValue(comb: mutable.ListBuffer[FeatureVector],
                 row: FeatureVector
                ): mutable.ListBuffer[FeatureVector] = {
    if(row.isCollection){
      mergeFeatureVectors(comb.head, row)
    } else {
      comb.append(row)
    }
    comb
  }

  def mergeCombiners(m1: mutable.ListBuffer[FeatureVector],
                     m2: mutable.ListBuffer[FeatureVector]
                    ): mutable.ListBuffer[FeatureVector] = {
    if(m1.head.isCollection){
      mergeFeatureVectors(m1.head, m2.head)
      m1
    } else {
      m1 ++ m2
    }
  }


  def extract(varObjs: Set[AttributeName], objArrs: Set[AttributeName], rows: RDD[JsonExplorerType]): RDD[(AttributeName,FeatureVector)] = {
    rows.flatMap(shredJET(varObjs,objArrs,_))
      .reduceByKey(_+_)
      .map(x => ((x._1._1,x._1._2.map(_._1).toSet),(x._1._1,x._1._2,x._2.toInt)))
      .combineByKey(createAttributeCombiner,mergeAttributeValue,mergeAttributeCombiner).map(x => ((x._1._1),FeatureVector(x._1._1,x._1._2,x._2,varObjs.contains(x._1._1))))
      .combineByKey(createCombiner,mergeValue,mergeCombiners).flatMap(x => x._2.map(y=>(x._1,y)))
  }


  case class FeatureVector(sourceSchema: AttributeName, localSchema: Set[AttributeName], typeCount: mutable.HashMap[AttributeName, mutable.HashMap[JsonExplorerType,Int]], isCollection: Boolean)

}
