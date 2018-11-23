package Seralize

import Explorer._
import com.google.gson.Gson
import scala.collection.JavaConverters._


object StringSeralizer {

  /** Preforms the initial serialization step
    * This dataframe is then cached and can be used during clustering for correlation
    * Also handles the initial conversion to our JsonExplorer types
    *
    */

  def seralize(rows: Iterator[String]): Iterator[String] = {
    val StringClass = classOf[String]
    val DoubleClass = classOf[java.lang.Double]
    val BooleanClass = classOf[java.lang.Boolean]
    val ArrayClass = classOf[java.util.ArrayList[_]]
    val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
    val MapType = new java.util.HashMap[String,Object]().getClass
    val ArrayType = new java.util.ArrayList[Object]().getClass

    val JE = Explorer.JE_String


    val ThrowUnknownTypeException = true
    val gson = new Gson()
    // local collector, collects attributeName and type and it's count.
    val mapped_rows: scala.collection.mutable.ListBuffer[String] = new scala.collection.mutable.ListBuffer[String]()

    // this is the recursive function that is responsible for updating the collector, can probably be replace instead of
    def extractTypesO(jsonMap: java.util.Map[String,Object]): String = {

      val local_objs: scala.collection.mutable.ListBuffer[(String,String)] = new scala.collection.mutable.ListBuffer[(String,String)]()

      jsonMap.asScala.foreach{case(name,obj) => {
        var attributeClass: Class[_ <: Object] = null
        try {
          attributeClass = obj.getClass
        } catch {
          case e: java.lang.NullPointerException => // do nothing
        }
        attributeClass match {
          case(StringClass) => local_objs.append((name,JE.id(JE_String).toString))
          case(DoubleClass) => local_objs.append((name,JE.id(JE_Numeric).toString))
          case(BooleanClass) => local_objs.append((name,JE.id(JE_Boolean).toString))
          case(null) => local_objs.append((name,JE.id(JE_Null).toString))
          case(ArrayClass) =>
            val attributeList = obj.asInstanceOf[java.util.ArrayList[java.lang.Object]]
            if(attributeList.isEmpty)
              local_objs.append((name,JE.id(JE_Empty_Array).toString))
            else{
              local_objs.append((name,extractTypesA(attributeList)))
            }
          case(ObjectClass) =>
            val t = gson.fromJson(gson.toJson(obj), MapType)
            if(t.isEmpty)
              local_objs.append((name,JE.id(JE_Empty_Object).toString))
            else{
              local_objs.append((name,extractTypesO(gson.fromJson(gson.toJson(obj), MapType))))
            }
          case _ =>
            if(ThrowUnknownTypeException)
              throw new UnknownTypeException("Unknown Type Found in Extractor MapType: " + attributeClass.toString())
            else
              local_objs.append((name,JE.id(JE_Unknown).toString))
        } // end match
      }}

      return s"{${local_objs.map(x => s""""${x._1}":${x._2}""").mkString(",")}}"

    }


    def extractTypesA(jsonList: java.util.ArrayList[Object]): String = {

      val local_objs: scala.collection.mutable.ListBuffer[String] = new scala.collection.mutable.ListBuffer[String]()

      jsonList.asScala.foreach{obj => {
        var attributeClass: Class[_ <: Object] = null
        try {
          attributeClass = obj.getClass
        } catch {
          case e: java.lang.NullPointerException => // do nothing
        }
        attributeClass match {
          case(StringClass) => local_objs.append(JE.id(JE_String).toString)
          case(DoubleClass) => local_objs.append(JE.id(JE_Numeric).toString)
          case(BooleanClass) => local_objs.append(JE.id(JE_Boolean).toString)
          case(null) => local_objs.append(JE.id(JE_Null).toString)
          case(ArrayClass) =>
            val attributeList = obj.asInstanceOf[java.util.ArrayList[java.lang.Object]]
            if(attributeList.isEmpty)
              local_objs.append(JE.id(JE_Empty_Array).toString)
            else{
              local_objs.append(extractTypesA(attributeList))
            }
          case(ObjectClass) =>
            val t = gson.fromJson(gson.toJson(obj), MapType)
            if(t.isEmpty)
              local_objs.append(JE.id(JE_Empty_Object).toString)
            else{
              local_objs.append(extractTypesO(gson.fromJson(gson.toJson(obj), MapType)))
            }
          case _ =>
            if(ThrowUnknownTypeException)
              throw new UnknownTypeException("Unknown Type Found in Extractor MapType: " + attributeClass.toString())
            else
              local_objs.append(JE.id(JE_Unknown).toString)
        } // end match
      }}

      return s"[${local_objs.mkString(",")}]"


    }

    while(rows.hasNext){
      val row = rows.next()
      val map: java.util.HashMap[String,Object] = gson.fromJson(row, MapType)
      mapped_rows.append(extractTypesO(map)) // this call does the updating as well
      // new row
    }
    return mapped_rows.iterator
  }

}
