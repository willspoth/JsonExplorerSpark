package Explorer

object FeatureVectorCreator {


  def extractFVSs(schemas: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],JsonExtractionSchema], row: JsonExplorerType): scala.collection.mutable.ListBuffer[FeatureVector] = {
    val fvs: scala.collection.mutable.ListBuffer[FeatureVector] = scala.collection.mutable.ListBuffer[FeatureVector]()
    extractFVS(scala.collection.mutable.ListBuffer[Any](), schemas, row, fvs)

    return fvs
  }


  /*
    This is a recursive function that takes in a row(JE_Object) and returns a FeatureVector
   */
  def extractFVS(prefix: scala.collection.mutable.ListBuffer[Any], schemas: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],JsonExtractionSchema], row: JsonExplorerType, fvs: scala.collection.mutable.ListBuffer[FeatureVector]): Unit = {


    def containsName(name: scala.collection.mutable.ListBuffer[Any], except: scala.collection.mutable.ListBuffer[Any]): Boolean = {
      if(name.equals(except))
        return false
      return schemas.contains(name)
    }

    def extract(name: scala.collection.mutable.ListBuffer[Any], jet: JsonExplorerType, fv: FeatureVector): Unit = {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object => fv.updateFeature(name,jet.id)
        case JE_Object(xs) =>
          if(containsName(name,fv.parentName)){ // this attribute is a separate
            extractFVS(name, schemas, jet, fvs)
          } else { // name is not separate schema
            if(name.nonEmpty) {
              fv.updateFeature(name,JE_Object.id)
            }
            xs.foreach(je => extract(name :+ je._1, je._2, fv))
          }
        case JE_Array(xs) =>
          if(containsName(name,fv.parentName)){ // this attribute is a separate
            extractFVS(name, schemas, jet, fvs)
          } else {
            if (name.nonEmpty) {
              fv.updateFeature(name, JE_Array.id)
            }
            xs.foreach(je => {
              extract(name :+ Star, je, fv)
            })
          }
      }

    }

    def getSchema(name: scala.collection.mutable.ListBuffer[Any]): JsonExtractionSchema = {
      schemas.get(name) match {
        case Some(s) => s
        case None => null
      }
    }

    row match {
      case obj: JE_Object =>
        val fv: FeatureVector = new FeatureVector(getSchema(prefix))
        extract(prefix,obj,fv)
        fv.libsvm = toLibSVMFormat(fv)
        fvs += fv
      case arr: JE_Array =>
        getSchema(prefix).naiveType match {
          case JE_Obj_Array =>
            arr.xs.foreach(jet => {
              val fv: FeatureVector = new FeatureVector(getSchema(prefix))
              extract(prefix,jet,fv)
              fv.libsvm = toLibSVMFormat(fv)
              fvs += fv
            })
          case _ => throw new Exception("Unexpected type in FVC.extractFVS: ")
        }

      case JE_Empty_Array | JE_Empty_Object | JE_Null =>

      case _ => throw new Exception("FeatureVectorCreator.extract expects a JE_Object type, found: " + row.getType())
    }

  }

  def addKey(fv:FeatureVector): (scala.collection.mutable.ListBuffer[Any],String) = (fv.parentName,toLibSVMFormat(fv))

  def toLibSVMFormat(fv: FeatureVector): String = {
    val fvs = fv.Features.map(_._2).zipWithIndex.map{case(f,idx) => {
      f match {
        case Some(x) => idx+1
        case None => 0
      }
    }}.filter(_ > 0)

    fvs.size.toString() + " " + fvs.mkString(":1 ") + ":1"
  }

  def collapseFVS(name: scala.collection.mutable.ListBuffer[Any], iter: Iterable[String]): (scala.collection.mutable.ListBuffer[Any], List[(String,Int)])= {
    val collector: scala.collection.mutable.HashMap[String,Int] = scala.collection.mutable.HashMap[String,Int]()
    iter.foreach(x => {
      collector.get(x) match {
        case Some(i) => collector.put(x,i+1)
        case None => collector.put(x,1)
      }
    })
    (name,collector.toList)
  }


}
