package util

import Extractor.{JsonExplorerType, Star}
import Extractor.Types.{AttributeName, DisjointNodes}

import scala.collection.mutable

object JsonSchemaToJsonTable {

  def convert(mergedSchemas: Map[AttributeName,DisjointNodes], collections: Set[AttributeName], arrayOfObjects: Set[AttributeName]): Unit = {

    val schema = scala.collection.mutable.Set[AttributeName]()

    mergedSchemas.map{case (attributeName, disjointNodes) => {
      disjointNodes.map(bimaxOutput => {
        bimaxOutput.map(schemas => {


          schemas.schema.foreach(x => {
            val types = schemas.types.get(x).get
            if(types.size != 1)
              sys.exit(1)

            if(types.head.isBasic() && !x.last.equals(Star) && x.forall(!_.toString.contains("&")) && x.forall(!_.toString.contains("/") && x.forall(!_.toString.contains("%"))&& x.forall(!_.toString.contains("+"))&& x.forall(!_.toString.contains(",")))) {
              if(schema.size < 950)
                schema.add(x)
            }

          })


        })
      })
    }}

    val columns = schema.map(x => s"""${pathToColumnName(x)} PATH '${pathToOraclePath(x)}',\n""").mkString("")//.mkString(",\n")
    //val add = schema.map(x => s"""${pathToColumnName(x)}""").filter(_.contains("cms_")).mkString(" +\n")//.mkString(",\n")
    //println(add)
    println(columns)

  }

  def pathToColumnName(name: scala.collection.mutable.ListBuffer[Any]): String = {
    if(name.isEmpty)
      return "root"
    val nameString = name.foldLeft("")((acc,n)=>{
      n match {
        case s:String => acc + s
          .replace('.','_').replace(':','_').replace('-','_').replace(' ','_').replace('&','_') +
          "_"
        case i: Int => acc + s"""[$i]"""
        case Star => acc + s"""[*]"""
      }
    })
    if(nameString.last.equals('.'))
      return nameString.substring(0,nameString.size-1)
    else
      return nameString
  }

  def pathToOraclePath(name: scala.collection.mutable.ListBuffer[Any]): String = {
    if(name.isEmpty)
      return "$"
    val nameString = name.foldLeft("$.")((acc,n)=>{
      n match {
        case s: String => acc + (if(s.contains(":") || s.contains("-") || s.contains(" ")) "\""+s+"\"" else s) + "."
        case i: Int => acc + s"""[$i]"""
        case Star => acc + s"""[*]"""
      }
    })
    if(nameString.last.equals('.'))
      return nameString.substring(0,nameString.size-1)
    else
      return nameString
  }


}
