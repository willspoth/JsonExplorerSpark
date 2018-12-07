package Explorer

object FeatureVectorCreator {

  /*
    This is a recursive function that takes in a row(JE_Object) and returns a FeatureVector
   */
  def extractFVS(prefix: scala.collection.mutable.ListBuffer[Any], schemas: scala.collection.mutable.ListBuffer[JsonExtractionSchema], row: JsonExplorerType): scala.collection.mutable.ListBuffer[FeatureVector] = {

    val fvs: scala.collection.mutable.ListBuffer[FeatureVector] = scala.collection.mutable.ListBuffer[FeatureVector]()

    def containsName(name: scala.collection.mutable.ListBuffer[Any], except: scala.collection.mutable.ListBuffer[Any]): Boolean = {
      if(name.equals(except))
        return false
      schemas.foldLeft(false){(col,s) => (s.parent.equals(name) || col)}
    }

    def extract(name: scala.collection.mutable.ListBuffer[Any], jet: JsonExplorerType, fv: FeatureVector): Unit = {
      if(containsName(name,fv.parentName)){ // this attribute is a separate
        val it = extractFVS(name, schemas, jet)
        if(it.nonEmpty)
          it.foreach(i => fvs += i)
      } else { // name is not separate schema
        jet match {
          case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object => fv.updateFeature(name,jet)
          case JE_Object(xs) =>
            if(name.nonEmpty) {
              fv.updateFeature(name,JE_Object(xs.map(je => {
                (je._1, je._2.getType())
              })))
            }
            xs.foreach(je => extract(name :+ je._1, je._2, fv))
          case JE_Array(xs) =>
            if(name.nonEmpty) {
              fv.updateFeature(name,JE_Array(xs.map(je => {je.getType()})))
            }
            xs.zipWithIndex.foreach(je => {
              extract(name :+ je._2, je._1, fv)
            })
        }
      }
    }

    def getSchema(name: scala.collection.mutable.ListBuffer[Any]): JsonExtractionSchema = {
      schemas.foreach(s => {
        if(s.parent.equals(name))
          return s
      })
      null
    }

    row match {
      case obj: JE_Object =>
        val fv: FeatureVector = new FeatureVector(getSchema(prefix))
        extract(prefix,obj,fv)
        fvs += fv
      case arr: JE_Array =>
        getSchema(prefix).naiveType match {
          case JE_Obj_Array =>
            arr.xs.foreach(jet => {
              val fv: FeatureVector = new FeatureVector(getSchema(prefix))
              extract(prefix,jet,fv)
              fvs += fv
            })
          case _ => throw new Exception("Unexpected type in FVC.extractFVS: ")
        }

      case JE_Empty_Array | JE_Empty_Object | JE_Null =>

      case _ => throw new Exception("FeatureVectorCreator.extract expects a JE_Object type, found: " + row.getType())
    }

    return fvs
  }

  def addKey(fv:FeatureVector): (scala.collection.mutable.ListBuffer[Any],FeatureVector) = (fv.parentName,fv)


}
