package Extractor

import Explorer._

object Extract {
  def ExtractAttributes(rows:Iterator[JsonExplorerType]): Iterator[JsonExtractionRoot] = {
    val root = JsonExtractionRoot()

    // helper function to update root
    def add(name: scala.collection.mutable.ListBuffer[Any], j: JsonExplorerType): Unit = {
      root.attributes.get(name) match {
        case Some(a) =>
          a.types.get(j) match {
            case Some(x) => a.types.put(j, x + 1)
            case None => a.types.put(j, 1)
          }

        case None =>
          val a = Attribute()
          a.name = name
          a.types.put(j, 1)
          root.attributes.put(name, a)
      }
    }

    // extracts the types and child types and adds them to root
    def extract(name: scala.collection.mutable.ListBuffer[Any],jet: JsonExplorerType): Unit = {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object => add(name,jet)
        case JE_Object(xs) =>
          if(name.nonEmpty) {
            add(name,JE_Object(xs.map(je => {
              (je._1, je._2.getType())
            })))
          }
          xs.foreach(je => extract(name :+ je._1, je._2))
        case JE_Array(xs) =>
          if(name.nonEmpty) {
            add(name,JE_Array(xs.map(je => {je.getType()})))
          }
          xs.zipWithIndex.foreach(je => {
            extract(name :+ je._2, je._1)
          })
      }
    }

    while(rows.hasNext){
      val row = rows.next()
      extract(scala.collection.mutable.ListBuffer[Any](),row)
    }

    Iterator(root)
  }


  def combineAllRoots(r1:JsonExtractionRoot,r2:JsonExtractionRoot): JsonExtractionRoot = {
    r2.attributes.foreach{case(name,attribute) => {
      r1.attributes.get(name) match {
        case Some(x) =>
          attribute.types.foreach{case(k,v) => {
            x.types.put(k,x.types.getOrElse(k,0)+v)
          }}
          r1.attributes.put(name,x)
        case None =>
          r1.attributes.put(name,attribute)
      }
    }}
    r1
  }

}
