import java.io._
import java.util.Calendar

import Extractor.Types.{AttributeName, BiMaxNode, BiMaxStruct, DisjointNodes}
import org.apache.spark.rdd.RDD
import util.NodeToJsonSchema
import Extractor._
import org.spark_project.dmg.pmml.OutputField.Algorithm
import util.Log
import org.apache.spark.sql.functions.col

import scala.collection.mutable


object SparkMain {


  def main(args: Array[String]): Unit = {

    Log.add("Date",Calendar.getInstance().getTime().toString)

    val config = util.CMDLineParser.readArgs(args) // Creates the Spark session with its config values.

    Log.add("input file",config.fileName.replace("\\","/"))

    val startTime = System.currentTimeMillis() // Start timer

    val calculateEntropy = true

    val (variableObjs, objArrs): (Set[AttributeName],Set[AttributeName]) = if(calculateEntropy) {
      RunExplorer.extractComplexSchemas(config,startTime)
    } else {
      (Set[AttributeName](),Set[AttributeName]())
    }


    val secondPassStart = System.currentTimeMillis()

    val shreddedRecords: RDD[JsonExplorerType] = RunExplorer.shredRecords(config.train)

    // create feature vectors, currently should work if schemas generated from subset of training data
    val featureVectors: Array[(AttributeName,Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]])] =
      shreddedRecords.flatMap(FeatureVectors.shredJET(variableObjs, objArrs,_))
        .combineByKey(x => FeatureVectors.createCombiner(variableObjs, objArrs,x),FeatureVectors.mergeValue,FeatureVectors.mergeCombiners).collect()



    val fvTime = System.currentTimeMillis()
    val fvRunTime = fvTime - secondPassStart

    Log.add("FV creation time",fvRunTime.toString())

    var algorithmSchema = ""

    if(config.runBiMax.equals(util.CMDLineParser.BiMax)){

      // BiMax algorithm
      val rawSchemas: Map[AttributeName,Types.DisjointNodes] = featureVectors.map(x => {
        x._2 match {
          case Left(l) => (x._1,BiMax.OurBiMax.bin(l),true)
          case Right(r) =>
            (x._1,
              mutable.Seq(mutable.Seq(
                BiMaxNode(Set[AttributeName](),Map[AttributeName,mutable.Set[JsonExplorerType]](),0,r.map(x => (Map[AttributeName,mutable.Set[JsonExplorerType]]((x._1,x._2._1)),x._2._2)).toList.to[mutable.ListBuffer])
              )), false // don't do bimax on var_objects
            )
        }

      })
        .map(x => if (x._3) (x._1,BiMax.OurBiMax.rewrite(x._2,config.fast)) else (x._1,x._2)).toMap

      //TODO track basic types from raw schemas

      // combine subset types for each attribute
      val mergedSchemas:  Map[AttributeName,DisjointNodes] = rawSchemas.map{case(name,djn) => (name,djn.map(bms => bms.map(NodeToJsonSchema.biMaxNodeTypeMerger(_))))}

      val variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)] = variableObjs
        .map(varObjName => {val m = mergedSchemas
          .map(djn => { val d = djn._2
            .flatMap(possibleSchemas => possibleSchemas
              .map(z => {
                val first = if (z.multiplicity > 0) z.types.get(varObjName) match {case Some(v) => (v,z.multiplicity) case None => (mutable.Set[JsonExplorerType](),0)} else (mutable.Set[JsonExplorerType](),0)
                val sec = if(z.subsets.nonEmpty) z.subsets.map(sub => if (sub._1.contains(varObjName)) (sub._1.get(varObjName).get,sub._2) else (mutable.Set[JsonExplorerType](),0)).reduce((l:(mutable.Set[JsonExplorerType],Int),r:(mutable.Set[JsonExplorerType],Int)) => (l._1 ++ r._1, l._2 + r._2))
                else (mutable.Set[JsonExplorerType](),0)
                (first._1 ++ sec._1,first._2+sec._2)
              })
            )
            d
          })

          (varObjName,m.flatten.reduce((l,r) => (l._1 ++ r._1, l._2 + r._2)))
        }).toMap


      def reduceTypes(s: mutable.Set[JsonExplorerType]): mutable.Set[JsonExplorerType] = {
        if(s.size == 1)
          return s
        val typesWithoutEmpty = s.filter( t => if (t.equals(JE_Empty_Object) && s.contains(JE_Object) || (t.equals(JE_Empty_Array) && s.contains(JE_Array))) false else true)
        val types = typesWithoutEmpty.filter( t => if (t.equals(JE_Null) && typesWithoutEmpty.size == 2) false else true)
        return types
      }

      val reducedMergedSchemas = mergedSchemas.map(x => (x._1,x._2.map(y => y.map(z => {
        BiMaxNode(z.schema,
          z.types.map(r => (r._1, reduceTypes(r._2))),
          z.multiplicity,
          z.subsets
        )

      }))))
      //util.JsonSchemaToJsonTable.convert(reducedMergedSchemas, variableObjs, objArrs)
      val JsonSchema: util.JsonSchema.JSS = util.NodeToJsonSchema.biMaxToJsonSchema(mergedSchemas,variableObjWithMult, objArrs)
      algorithmSchema = JsonSchema.toString  + "\n"
    } else if(config.runBiMax.equals(util.CMDLineParser.Subset)) {

      // onlySubSet test
      val onlySubset: Map[AttributeName, Types.DisjointNodes] = featureVectors.map(x => {
        x._2 match {
          case Left(l) => (x._1, BiMax.OurBiMax.bin(l), true)
          case Right(r) =>
            (x._1,
              mutable.Seq(mutable.Seq(
                BiMaxNode(Set[AttributeName](), Map[AttributeName, mutable.Set[JsonExplorerType]](), 0, r.map(x => (Map[AttributeName, mutable.Set[JsonExplorerType]]((x._1, x._2._1)), x._2._2)).toList.to[mutable.ListBuffer])
              )), false // don't do bimax on var_objects
            )
        }

      })
        .map(x => (x._1, x._2)).toMap

      val variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)] = variableObjs
        .map(varObjName => {val m = onlySubset
          .map(djn => { val d = djn._2
            .flatMap(possibleSchemas => possibleSchemas
              .map(z => {
                val first = if (z.multiplicity > 0) z.types.get(varObjName) match {case Some(v) => (v,z.multiplicity) case None => (mutable.Set[JsonExplorerType](),0)} else (mutable.Set[JsonExplorerType](),0)
                val sec = if(z.subsets.nonEmpty) z.subsets.map(sub => if (sub._1.contains(varObjName)) (sub._1.get(varObjName).get,sub._2) else (mutable.Set[JsonExplorerType](),0)).reduce((l:(mutable.Set[JsonExplorerType],Int),r:(mutable.Set[JsonExplorerType],Int)) => (l._1 ++ r._1, l._2 + r._2))
                else (mutable.Set[JsonExplorerType](),0)
                (first._1 ++ sec._1,first._2+sec._2)
              })
            )
            d
          })

          (varObjName,m.flatten.reduce((l,r) => (l._1 ++ r._1, l._2 + r._2)))
        }).toMap

      val JsonSchema: util.JsonSchema.JSS = util.NodeToJsonSchema.biMaxToJsonSchema(onlySubset,variableObjWithMult, objArrs)
      algorithmSchema = JsonSchema.toString  + "\n"
    } else if(config.runBiMax.equals(util.CMDLineParser.Verbose)){
      val rawSchemas: Map[AttributeName,Types.DisjointNodes] = featureVectors.map(x => {
        x._2 match {
          case Left(l) =>
            (x._1,
              mutable.Seq[BiMaxStruct](l.map(x => BiMaxNode(
                x._1.map(_._1).toSet,
                x._1,
                x._2,
                mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int)]()
              )).toSeq.to[mutable.Seq]
              )
            )

          case Right(r) =>
            (x._1,
              mutable.Seq(mutable.Seq(
                BiMaxNode(Set[AttributeName](),Map[AttributeName,mutable.Set[JsonExplorerType]](),0,r.map(x => (Map[AttributeName,mutable.Set[JsonExplorerType]]((x._1,x._2._1)),x._2._2)).toList.to[mutable.ListBuffer])
              )) // don't do bimax on var_objects
            )
        }

      }).toMap

      val variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)] = variableObjs
        .map(varObjName => {val m = rawSchemas
          .map(djn => { val d = djn._2
            .flatMap(possibleSchemas => possibleSchemas
              .map(z => {
                val first = if (z.multiplicity > 0) z.types.get(varObjName) match {case Some(v) => (v,z.multiplicity) case None => (mutable.Set[JsonExplorerType](),0)} else (mutable.Set[JsonExplorerType](),0)
                val sec = if(z.subsets.nonEmpty) z.subsets.map(sub => if (sub._1.contains(varObjName)) (sub._1.get(varObjName).get,sub._2) else (mutable.Set[JsonExplorerType](),0)).reduce((l:(mutable.Set[JsonExplorerType],Int),r:(mutable.Set[JsonExplorerType],Int)) => (l._1 ++ r._1, l._2 + r._2))
                else (mutable.Set[JsonExplorerType](),0)
                (first._1 ++ sec._1,first._2+sec._2)
              })
            )
            d
          })

          (varObjName,m.flatten.reduce((l,r) => (l._1 ++ r._1, l._2 + r._2)))
        }).toMap

      val JsonSchema: util.JsonSchema.JSS = util.NodeToJsonSchema.biMaxToJsonSchema(rawSchemas,variableObjWithMult, objArrs)
      algorithmSchema = JsonSchema.toString  + "\n"
    } else if(config.runBiMax.equals(util.CMDLineParser.kmeans) || config.runBiMax.equals(util.CMDLineParser.Hierarchical)){
      // BiMax algorithm
      val k = 6

      val rawSchemas: Map[AttributeName,Types.DisjointNodes] = featureVectors.map(x => {
        x._2 match {
          case Left(l) =>
            //Type mismatch. Required: String, found: mutable.HashMap[Map[AttributeName, mutable.Set[JsonExplorerType]], Int]
            //Type mismatch. Required: String, found: mutable.HashMap[AttributeName, (mutable.Set[JsonExplorerType], Int)]
            if(x._1.isEmpty) { // only do k-means on root to give it the best chance
              (x._1,Exec.Algorithms.toDisjointNodes(l),true)
            } else {
              val attributeMap: mutable.HashMap[AttributeName, (mutable.Set[JsonExplorerType], Int)] = mutable.HashMap[AttributeName, (mutable.Set[JsonExplorerType], Int)]()
              l.foreach{case(row,count) =>
                row.foreach{case(name,types) => {
                  attributeMap.get(name) match {
                    case Some(attributeStats) =>
                      types.foreach(typ => {
                        if(!attributeStats._1.contains(typ))
                          attributeStats._1.add(typ)
                      })
                      attributeMap.put(name,(attributeStats._1,attributeStats._2+count))
                    case None =>
                      attributeMap.put(name,(types,count))
                  }
                }}
              }
              (x._1,
                mutable.Seq(mutable.Seq(
                  BiMaxNode(Set[AttributeName](),Map[AttributeName,mutable.Set[JsonExplorerType]](),0,attributeMap.map(x => (Map[AttributeName,mutable.Set[JsonExplorerType]]((x._1,x._2._1)),x._2._2)).toList.to[mutable.ListBuffer])
                )), false // flatten non-root
              )
            }
          case Right(r) =>
            (x._1,
              mutable.Seq(mutable.Seq(
                BiMaxNode(Set[AttributeName](),Map[AttributeName,mutable.Set[JsonExplorerType]](),0,r.map(x => (Map[AttributeName,mutable.Set[JsonExplorerType]]((x._1,x._2._1)),x._2._2)).toList.to[mutable.ListBuffer])
              )), false // flatten var_objects
            )
        }

      })
        .map(x => if (x._3) (x._1,
          if(config.runBiMax.equals(util.CMDLineParser.kmeans)) Exec.Algorithms.runKMeans(config.spark.sparkContext,x._2,k) else Exec.Algorithms.runHierachical(config.spark.sparkContext,x._2,k)
        ) else (x._1,x._2)).toMap

      //TODO track basic types from raw schemas

      // combine subset types for each attribute
      val mergedSchemas:  Map[AttributeName,DisjointNodes] = rawSchemas.map{case(name,djn) => (name,djn.map(bms => bms.map(NodeToJsonSchema.biMaxNodeTypeMerger(_))))}

      val variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)] = variableObjs
        .map(varObjName => {val m = mergedSchemas
          .map(djn => { val d = djn._2
            .flatMap(possibleSchemas => possibleSchemas
              .map(z => {
                val first = if (z.multiplicity > 0) z.types.get(varObjName) match {case Some(v) => (v,z.multiplicity) case None => (mutable.Set[JsonExplorerType](),0)} else (mutable.Set[JsonExplorerType](),0)
                val sec = if(z.subsets.nonEmpty) z.subsets.map(sub => if (sub._1.contains(varObjName)) (sub._1.get(varObjName).get,sub._2) else (mutable.Set[JsonExplorerType](),0)).reduce((l:(mutable.Set[JsonExplorerType],Int),r:(mutable.Set[JsonExplorerType],Int)) => (l._1 ++ r._1, l._2 + r._2))
                else (mutable.Set[JsonExplorerType](),0)
                (first._1 ++ sec._1,first._2+sec._2)
              })
            )
            d
          })

          (varObjName,m.flatten.reduce((l,r) => (l._1 ++ r._1, l._2 + r._2)))
        }).toMap


      def reduceTypes(s: mutable.Set[JsonExplorerType]): mutable.Set[JsonExplorerType] = {
        if(s.size == 1)
          return s
        val typesWithoutEmpty = s.filter( t => if (t.equals(JE_Empty_Object) && s.contains(JE_Object) || (t.equals(JE_Empty_Array) && s.contains(JE_Array))) false else true)
        val types = typesWithoutEmpty.filter( t => if (t.equals(JE_Null) && typesWithoutEmpty.size == 2) false else true)
        return types
      }

      val reducedMergedSchemas = mergedSchemas.map(x => (x._1,x._2.map(y => y.map(z => {
        BiMaxNode(z.schema,
          z.types.map(r => (r._1, reduceTypes(r._2))),
          z.multiplicity,
          z.subsets
        )

      }))))
      //util.JsonSchemaToJsonTable.convert(reducedMergedSchemas, variableObjs, objArrs)
      val JsonSchema: util.JsonSchema.JSS = util.NodeToJsonSchema.biMaxToJsonSchema(mergedSchemas,variableObjWithMult, objArrs)
      algorithmSchema = JsonSchema.toString  + "\n"
    }
    else if(config.runBiMax.equals(util.CMDLineParser.Flat)){
      // BiMax algorithm
      val rawSchemas: Map[AttributeName,Types.DisjointNodes] = featureVectors.map(x => {
        x._2 match {
          case Left(l) =>
              val attributeMap: mutable.HashMap[AttributeName, (mutable.Set[JsonExplorerType], Int)] = mutable.HashMap[AttributeName, (mutable.Set[JsonExplorerType], Int)]()
              l.foreach{case(row,count) =>
                row.foreach{case(name,types) => {
                  attributeMap.get(name) match {
                    case Some(attributeStats) =>
                      types.foreach(typ => {
                        if(!attributeStats._1.contains(typ))
                          attributeStats._1.add(typ)
                      })
                      attributeMap.put(name,(attributeStats._1,attributeStats._2+count))
                    case None =>
                      attributeMap.put(name,(types,count))
                  }
                }}
              }
              (x._1,
                mutable.Seq(mutable.Seq(
                  BiMaxNode(Set[AttributeName](),Map[AttributeName,mutable.Set[JsonExplorerType]](),0,attributeMap.map(x => (Map[AttributeName,mutable.Set[JsonExplorerType]]((x._1,x._2._1)),x._2._2)).toList.to[mutable.ListBuffer])
                )), false // flatten non-root
              )

          case Right(r) =>
            (x._1,
              mutable.Seq(mutable.Seq(
                BiMaxNode(Set[AttributeName](),Map[AttributeName,mutable.Set[JsonExplorerType]](),0,r.map(x => (Map[AttributeName,mutable.Set[JsonExplorerType]]((x._1,x._2._1)),x._2._2)).toList.to[mutable.ListBuffer])
              )), false // flatten var_objects
            )
        }

      })
        .map(x => if (x._3) (x._1,Exec.Algorithms.runKMeans(config.spark.sparkContext,x._2,10)) else (x._1,x._2)).toMap

      //TODO track basic types from raw schemas

      // combine subset types for each attribute
      val mergedSchemas:  Map[AttributeName,DisjointNodes] = rawSchemas.map{case(name,djn) => (name,djn.map(bms => bms.map(NodeToJsonSchema.biMaxNodeTypeMerger(_))))}

      val variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)] = variableObjs
        .map(varObjName => {val m = mergedSchemas
          .map(djn => { val d = djn._2
            .flatMap(possibleSchemas => possibleSchemas
              .map(z => {
                val first = if (z.multiplicity > 0) z.types.get(varObjName) match {case Some(v) => (v,z.multiplicity) case None => (mutable.Set[JsonExplorerType](),0)} else (mutable.Set[JsonExplorerType](),0)
                val sec = if(z.subsets.nonEmpty) z.subsets.map(sub => if (sub._1.contains(varObjName)) (sub._1.get(varObjName).get,sub._2) else (mutable.Set[JsonExplorerType](),0)).reduce((l:(mutable.Set[JsonExplorerType],Int),r:(mutable.Set[JsonExplorerType],Int)) => (l._1 ++ r._1, l._2 + r._2))
                else (mutable.Set[JsonExplorerType](),0)
                (first._1 ++ sec._1,first._2+sec._2)
              })
            )
            d
          })

          (varObjName,m.flatten.reduce((l,r) => (l._1 ++ r._1, l._2 + r._2)))
        }).toMap


      def reduceTypes(s: mutable.Set[JsonExplorerType]): mutable.Set[JsonExplorerType] = {
        if(s.size == 1)
          return s
        val typesWithoutEmpty = s.filter( t => if (t.equals(JE_Empty_Object) && s.contains(JE_Object) || (t.equals(JE_Empty_Array) && s.contains(JE_Array))) false else true)
        val types = typesWithoutEmpty.filter( t => if (t.equals(JE_Null) && typesWithoutEmpty.size == 2) false else true)
        return types
      }

      val reducedMergedSchemas = mergedSchemas.map(x => (x._1,x._2.map(y => y.map(z => {
        BiMaxNode(z.schema,
          z.types.map(r => (r._1, reduceTypes(r._2))),
          z.multiplicity,
          z.subsets
        )

      }))))
      //util.JsonSchemaToJsonTable.convert(reducedMergedSchemas, variableObjs, objArrs)
      val JsonSchema: util.JsonSchema.JSS = util.NodeToJsonSchema.biMaxToJsonSchema(mergedSchemas,variableObjWithMult, objArrs)
      algorithmSchema = JsonSchema.toString  + "\n"
    } else {
      throw new Exception("Unknown Merge algorithm choice")
    }


    val endTime = System.currentTimeMillis() // End Timer


    //log += LogOutput("FVTime",fvRunTime.toString,"FV Creation Took: "," ms")
    Log.add("total time",(endTime - startTime).toString)
    Log.add("train percent",config.trainPercent.toString)
    Log.add("validation size",config.validationSize.toString)
    Log.add("algorithm",config.runBiMax.toString())
    Log.add("seed",config.seed match {
      case Some(i) => i.toString
      case None => "None"})

    config.spark.conf.getAll.foreach{case(k,v) => Log.add(k,v)}
    Log.add("KSE",config.kse.toString)

//    println(SizeEstimator.estimate(featureVectors.filter(x=> x._1.isEmpty || attributeMap.get(x._1).get.`type`.contains(JE_Var_Object))))
//    println(SizeEstimator.estimate(featureVectors))

    Log.writeLog(config.logFileName, if(config.writeJsonSchema) algorithmSchema else "")
    Log.printLog()

  }

}

// Row Counts:
// Medicine: 239,930
// Github: 3,321,596
// Yelp: 7,437,120
// Business: 156,639
// Checkin: 135,148
// Photos: 196,278
// Review: 4,736,897
// Tip: 1,028,802
// User: 1,183,362
// Synapse: 147847
// Twitter: 808,442
// NYT2019: 69116
// WikiData: 16980683 -- 1700614