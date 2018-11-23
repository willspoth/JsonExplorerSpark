package Extractor

import com.google.gson.Gson


import Explorer._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object ExtractorPhase {

  // returns a list of S
  // uses map Partitions
  def mapTypes(rows: Iterator[String]):Iterator[(String,scala.collection.mutable.HashMap[JsonExplorerType,Int])] = {
    val StringClass = classOf[String]
    val DoubleClass = classOf[java.lang.Double]
    val BooleanClass = classOf[java.lang.Boolean]
    val ArrayClass = classOf[java.util.ArrayList[_]]
    val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
    val MapType = new java.util.HashMap[String,Object]().getClass


    val ThrowUnknownTypeException = true
    val gson = new Gson()
    // local collector, collects attributeName and type and it's count.
    val collector: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[JsonExplorerType,Int]] = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[JsonExplorerType,Int]]()

    // simply returns the type of an object as a JsonExplorerType
    def getType(attributeValue: Object): JsonExplorerType = {
      var attributeClass: Class[_ <: Object] = null
      try {
        attributeClass = attributeValue.getClass
      } catch {
        case e: java.lang.NullPointerException => // do nothing
      }
      attributeClass match {
        // return basic types
        case(StringClass) => return JE_String
        case(DoubleClass) => return JE_Numeric
        case(BooleanClass) => return JE_Boolean
        case(null) => return JE_Null
        // return array, or empty array
        case(ArrayClass) =>
          val attributeList = attributeValue.asInstanceOf[java.util.ArrayList[java.lang.Object]].asScala
          if(attributeList.isEmpty)
            return JE_Empty_Array
          else
            return JE_Array
        case(ObjectClass) =>
          val t = gson.fromJson(gson.toJson(attributeValue), MapType)
          if(t.isEmpty)
            return JE_Empty_Object
          else
            return JE_Object
        case _ =>
          if(ThrowUnknownTypeException)
            throw new UnknownTypeException("Unknown Type Found in Extractor MapType: " + attributeClass.toString())
          else
            return JE_Unknown
      }
    }

    // this is the recursive function that is responsible for updating the collector, can probably be replace instead of
    def extractTypes(prefix: String, jsonMap: java.util.HashMap[String,Object]): Unit = {
      jsonMap.asScala.foreach{case(name,obj) => {
        getType(obj) match {
          case(JE_String) => updateCollector(prefix+name,JE_String)
          case(JE_Numeric) => updateCollector(prefix+name,JE_Numeric)
          case(JE_Boolean) => updateCollector(prefix+name,JE_Boolean)
          case(JE_Null) => updateCollector(prefix+name,JE_Null)
          case(JE_Empty_Array) => updateCollector(prefix+name,JE_Empty_Array)
          case(JE_Empty_Object) => updateCollector(prefix+name,JE_Empty_Object)
          case(JE_Unknown) => updateCollector(prefix+name,JE_Empty_Object)

          case(JE_Array) =>
            val attributeList: scala.collection.mutable.Buffer[Object] = obj.asInstanceOf[java.util.ArrayList[java.lang.Object]].asScala
            updateCollector(prefix+name,JE_Array(attributeList.map(o=>{
              getType(o)
            }).toList))
            val tempMap: java.util.HashMap[String,Object] = new java.util.HashMap[String,Object]()
            attributeList.zipWithIndex.foreach{ case(o,idx) => {
              tempMap.put(s"[$idx]",o)
            }}
            extractTypes(prefix+name,tempMap)
          case(JE_Object) =>
            val t: java.util.HashMap[String,Object] = gson.fromJson(gson.toJson(obj), MapType)
            updateCollector(prefix+name,JE_Object(t.asScala.map{case(n,o) => {
              (prefix+name+'.'+n,getType(o))
            }}.toMap))
            extractTypes(prefix+name+".",t)
        }
      }}
    }

    def updateCollector(name: String, typ: JsonExplorerType): Unit = {
      collector.get(name) match {
        case Some(typeMap) =>
          typeMap.get(typ) match {
            case Some(i) =>
              typeMap.put(typ,i+1)
              collector.put(name,typeMap)
            case None =>
              typeMap.put(typ,1)
              collector.put(name,typeMap)
          }
        case None =>
          val tempMap = new scala.collection.mutable.HashMap[JsonExplorerType,Int]()
          tempMap.put(typ,1)
          collector.put(name,tempMap)
      }
    }

    while(rows.hasNext){
      val row = rows.next()
      val map: java.util.HashMap[String,Object] = gson.fromJson(row, MapType)
      extractTypes("",map) // this call does the updating as well
      // new row
    }
    return collector.toList.iterator
  }


  def reduceTypes(col: scala.collection.mutable.HashMap[JsonExplorerType,Int], tup: scala.collection.mutable.HashMap[JsonExplorerType,Int]): scala.collection.mutable.HashMap[JsonExplorerType,Int] = {
    var small: scala.collection.mutable.HashMap[JsonExplorerType,Int] = null
    var large: scala.collection.mutable.HashMap[JsonExplorerType,Int] = null
    if(col.size <= tup.size){
      small = col
      large = tup
    } else {
      small = tup
      large = col
    }

    col.foreach{case(typ,count) => {
      large.get(typ) match {
        case Some(prevCount) => large.put(typ,prevCount+count)
        case None => large.put(typ,count)
      }
    }}
    return large
  }






  // same as mapTypes but doesn't implement the collector, this is because ReduceByKey already does this
  def mapTypesSimple(rows: Iterator[String]):Iterator[(String,JsonExplorerType)] = {
    val StringClass = classOf[String]
    val DoubleClass = classOf[java.lang.Double]
    val BooleanClass = classOf[java.lang.Boolean]
    val ArrayClass = classOf[java.util.ArrayList[_]]
    val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
    val MapType = new java.util.HashMap[String,Object]().getClass


    val ThrowUnknownTypeException = true
    val gson = new Gson()
    // local collector, collects attributeName and type and it's count.
    val return_list: ListBuffer[(String,JsonExplorerType)] = ListBuffer[(String,JsonExplorerType)]()

    // simply returns the type of an object as a JsonExplorerType
    def getType(attributeValue: Object): JsonExplorerType = {
      var attributeClass: Class[_ <: Object] = null
      try {
        attributeClass = attributeValue.getClass
      } catch {
        case e: java.lang.NullPointerException => // do nothing
      }
      attributeClass match {
        // return basic types
        case(StringClass) => return JE_String
        case(DoubleClass) => return JE_Numeric
        case(BooleanClass) => return JE_Boolean
        case(null) => return JE_Null
        // return array, or empty array
        case(ArrayClass) =>
          val attributeList = attributeValue.asInstanceOf[java.util.ArrayList[java.lang.Object]].asScala
          if(attributeList.isEmpty)
            return JE_Empty_Array
          else
            return JE_Array
        case(ObjectClass) =>
          val t = gson.fromJson(gson.toJson(attributeValue), MapType)
          if(t.isEmpty)
            return JE_Empty_Object
          else
            return JE_Object
        case _ =>
          if(ThrowUnknownTypeException)
            throw new UnknownTypeException("Unknown Type Found in Extractor MapType: " + attributeClass.toString())
          else
            return JE_Unknown
      }
    }

    // this is the recursive function that is responsible for updating the collector, can probably be replace instead of
    def extractTypes(prefix: String, jsonMap: java.util.HashMap[String,Object]): Unit = {
      jsonMap.asScala.foreach{case(name,obj) => {
        getType(obj) match {
          case(JE_String) => return_list.append((prefix+name,JE_String))
          case(JE_Numeric) => return_list.append((prefix+name,JE_Numeric))
          case(JE_Boolean) => return_list.append((prefix+name,JE_Boolean))
          case(JE_Null) => return_list.append((prefix+name,JE_Null))
          case(JE_Empty_Array) => return_list.append((prefix+name,JE_Empty_Array))
          case(JE_Empty_Object) => return_list.append((prefix+name,JE_Empty_Object))
          case(JE_Unknown) => return_list.append((prefix+name,JE_Empty_Object))

          case(JE_Array) =>
            val attributeList: scala.collection.mutable.Buffer[Object] = obj.asInstanceOf[java.util.ArrayList[java.lang.Object]].asScala
            return_list.append((prefix+name,JE_Array(attributeList.map(o=>{
              getType(o)
            }).toList)))
            val tempMap: java.util.HashMap[String,Object] = new java.util.HashMap[String,Object]()
            attributeList.zipWithIndex.foreach{ case(o,idx) => {
              tempMap.put(s"[$idx]",o)
            }}
            extractTypes(prefix+name,tempMap)
          case(JE_Object) =>
            val t: java.util.HashMap[String,Object] = gson.fromJson(gson.toJson(obj), MapType)
            return_list.append((prefix+name,JE_Object(t.asScala.map{case(n,o) => {
              (prefix+name+'.'+n,getType(o))
            }}.toMap)))
            extractTypes(prefix+name+".",t)
        }
      }}
    }

    while(rows.hasNext){
      val row = rows.next()
      val map: java.util.HashMap[String,Object] = gson.fromJson(row, MapType)
      extractTypes("",map) // this call does the updating as well
      // new row
    }

    return return_list.iterator
  }


}
