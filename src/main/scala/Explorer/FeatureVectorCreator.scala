package Explorer

import breeze.linalg.{DenseMatrix, DenseVector}

import scala.collection.mutable.ArrayBuffer

object FeatureVectorCreator {


  def extractFVSs(schemas: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],JsonExtractionSchema], row: JsonExplorerType): scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]] = {
    val fvs: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]] = scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]]()
    extractFVS(scala.collection.mutable.ListBuffer[Any](), schemas, row, fvs, scala.collection.mutable.ListBuffer[Any]())
    return fvs
  }


  /*
    This is a recursive function that takes in a row(JE_Object) and returns a FeatureArrayBuffer
   */
  def extractFVS(prefix: scala.collection.mutable.ListBuffer[Any], schemas: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],JsonExtractionSchema], row: JsonExplorerType, fvs: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]], currentSchema: scala.collection.mutable.ListBuffer[Any]): Unit = {


    def getSchema(name: scala.collection.mutable.ListBuffer[Any]): JsonExtractionSchema = {
      schemas.get(name) match {
        case Some(s) => s
        case None => null
      }
    }

    val schema: JsonExtractionSchema = getSchema(prefix)

    def containsName(name: scala.collection.mutable.ListBuffer[Any], except: scala.collection.mutable.ListBuffer[Any]): Boolean = {
      if(name.equals(except))
        return false
      return schemas.contains(name)
    }

    def extract(name: scala.collection.mutable.ListBuffer[Any], jet: JsonExplorerType, fv: ArrayBuffer[Byte]): Unit = {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
          if (name.nonEmpty && !name.equals(currentSchema))
            fv(schema.attributeLookup.get(name).get) = 1
        case JE_Object(xs) =>
          if(containsName(name,currentSchema)){ // this attribute is a separate
            fv(schema.attributeLookup.get(name).get) = 1
            extractFVS(name, schemas, jet, fvs, name)
          } else { // name is not separate schema
            if (name.nonEmpty && !name.equals(currentSchema)) {
              fv(schema.attributeLookup.get(name).get) = 1
            }
            xs.foreach(je => extract(name :+ je._1, je._2, fv))
          }
        case JE_Array(xs) =>
          if(containsName(name,currentSchema)){ // this attribute is a separate
            fv(schema.attributeLookup.get(name).get) = 1
            extractFVS(name, schemas, jet, fvs, name)
          } else {
            if (name.nonEmpty && !name.equals(currentSchema)) {
              fv(schema.attributeLookup.get(name).get) = 1
            }
            xs.foreach(je => {
              extract(name :+ Star, je, fv)
            })
          }
      }

    }



    row match {
      case obj: JE_Object =>
        val fv: ArrayBuffer[Byte] = ArrayBuffer.fill[Byte](schema.attributes.size)(0)
        extract(prefix,obj,fv)
        fvs.get(currentSchema) match {
          case Some(s) =>
            s.get(fv) match {
              case Some(c) => s.put(fv,c)
              case None => s.put(fv,1)
            }
          case None =>
            val t = scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]()
            t.put(fv,1)
            fvs.put(currentSchema,t)
        }
      case arr: JE_Array =>
        schema.naiveType match {
          case JE_Obj_Array =>
            arr.xs.foreach(jet => {
              val fv: ArrayBuffer[Byte] = ArrayBuffer.fill[Byte](schema.attributes.size)(0)
              extract(prefix:+ Star,jet,fv)
              fvs.get(currentSchema) match {
                case Some(s) =>
                  s.get(fv) match {
                    case Some(c) => s.put(fv,c)
                    case None => s.put(fv,1)
                  }
                case None =>
                  val t = scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]()
                  t.put(fv,1)
                  fvs.put(currentSchema,t)
              }
            })
          case _ => throw new Exception("Unexpected type in FVC.extractFVS: ")
        }

      case JE_Empty_Array | JE_Empty_Object | JE_Null =>

      case _ => throw new Exception("FeatureArrayBufferCreator.extract expects a JE_Object type, found: " + row.getType())
    }

  }
/*
  def addKey(fv:FeatureArrayBuffer): (scala.collection.mutable.ListBuffer[Any],String) = (fv.parentName,toLibSVMFormat(fv))

  def toLibSVMFormat(fv: FeatureArrayBuffer): String = {
    val fvs = fv.Features.map(_._2).zipWithIndex.map{case(f,idx) => {
      f match {
        case Some(x) => idx+1
        case None => 0
      }
    }}.filter(_ > 0)

    fvs.size.toString() + " " + fvs.mkString(":1 ") + ":1"
  }
*/
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

  def Combine(l: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int], r: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int]): scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int] = {
    if(l.size >= r.size){ // copy right into left
      r.foreach{case(n,c1) => {
        l.get(n) match {
          case Some(c2) => l.put(n,c1+c2)
          case None => l.put(n,c1)
        }
      }}
      return l
    } else { // copy left into right
      l.foreach{case(n,c1) => {
        r.get(n) match {
          case Some(c2) => r.put(n,c1+c2)
          case None => r.put(n,c1)
        }
      }}
      return r
    }
  }

  def toDense(name: scala.collection.mutable.ListBuffer[Any], m: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int]): (scala.collection.mutable.ListBuffer[Any],DenseMatrix[Double],DenseVector[Double]) = {
    val t = m.toList.unzip[ArrayBuffer[Byte],Int]
    val fvs: DenseMatrix[Double] = new DenseMatrix[Double](t._1.size,t._1(0).size,(t._1.map(_.map(_.toDouble).toList)).flatten.toArray)
    val mults: DenseVector[Double] = new DenseVector[Double](t._2.map(_.toDouble).toArray)
    (name,fvs,mults)
  }

}
