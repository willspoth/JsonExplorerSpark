package Explorer

import Explorer.Types.{AttributeName, SchemaName}
import breeze.linalg.{DenseMatrix, DenseVector}

import scala.collection.mutable.ArrayBuffer

/** Use the plan to generate localized feature vectors, similar to extract but needs to pivot arrays of objects.
  *
  */
object FeatureVectorCreator {



  def extractFVSs(schemas: scala.collection.mutable.HashMap[SchemaName,JsonExtractionSchema], row: JsonExplorerType): scala.collection.mutable.HashMap[SchemaName,scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]] = {
    val fvs: scala.collection.mutable.HashMap[SchemaName,scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]] = scala.collection.mutable.HashMap[SchemaName,scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]]()
    extractFVS(new AttributeName(), schemas, row, fvs, new SchemaName())
    return fvs
  }


  /*
    This is a recursive function that takes in a row(JE_Object) and returns a FeatureArrayBuffer. ToDo pick better name and make more efficient
   */
  private def extractFVS(prefix: AttributeName, schemas: scala.collection.mutable.HashMap[SchemaName,JsonExtractionSchema], row: JsonExplorerType, fvs: scala.collection.mutable.HashMap[SchemaName,scala.collection.mutable.HashMap[ArrayBuffer[Byte],Int]], currentSchema: SchemaName): Unit = {

    def getSchema(name: AttributeName): JsonExtractionSchema = {
      schemas.get(name) match {
        case Some(s) => s
        case None => null
      }
    }

    val schema: JsonExtractionSchema = getSchema(prefix)

    def containsName(name: AttributeName, except: AttributeName): Boolean = {
      if(name.equals(except))
        return false
      return schemas.contains(name)
    }

    def extract(name: AttributeName, jet: JsonExplorerType, fv: ArrayBuffer[Byte]): Unit = {
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
            xs.foreach(je =>
              extract(name ++ List(je._1), je._2, fv))
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
              extract(name ++ List(Star), je, fv)
            })
          }
      }


      // List :::

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
              extract(prefix ++ List(Star),jet,fv)
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

  def collapseFVS(name: AttributeName, iter: Iterable[String]): (AttributeName, List[(String,Int)])= {
    val collector: scala.collection.mutable.HashMap[String,Int] = scala.collection.mutable.HashMap[String,Int]()
    iter.foreach(x => {
      collector.get(x) match {
        case Some(i) => collector.put(x,i+1)
        case None => collector.put(x,1)
      }
    })
    (name,collector.toList)
  }



  def toDense(name: AttributeName, m: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int]): (AttributeName,DenseMatrix[Double],DenseVector[Double]) = {
    val t = m.toList.unzip[ArrayBuffer[Byte],Int]
    val fvs: DenseMatrix[Double] = new DenseMatrix[Double](t._1.size,t._1(0).size,t._1.flatten.toArray.map(_.toDouble))
    val mults: DenseVector[Double] = new DenseVector[Double](t._2.toArray.map(_.toDouble))
    (name,fvs,mults)
  }

  def createCombiner(fv: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int]): scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int] = {
    fv
  }

  def mergeValue(comb: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int], v: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int]): scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int] = {
    v.foreach{case(fv, c) => {
      comb.get(fv) match {
        case Some(count2) => comb.update(fv,c+count2)
        case None => comb.put(fv,c)
      }
    }}
    comb
  }

  def mergeCombiners(c1: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int], c2: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int]): scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int] = {
    if(c1.size >= c2.size){
      c2.foreach{case(n,t) => {
        c1.get(n) match {
          case Some(v) => c1.update(n,v+t)
          case None => c1.put(n,t)
        }
      }}
      c1
    } else {
      c1.foreach{case(n,t) => {
        c2.get(n) match {
          case Some(v) => c2.update(n,v+t)
          case None => c2.put(n,t)
        }
      }}
      c2
    }
  }

}
