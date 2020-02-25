package JsonExplorer

import java.io._
import java.util.Calendar

import Explorer.Types.{AttributeName, BiMaxNode, BiMaxStruct, DisjointNodes}
import org.apache.spark.rdd.RDD
import util.NodeToJsonSchema
import Explorer._

import scala.collection.mutable


object SparkMain {


  def main(args: Array[String]): Unit = {

    val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()
    log += LogOutput("Date",Calendar.getInstance().getTime().toString,"Date: ")

    val config = util.CMDLineParser.readArgs(args) // Creates the Spark session with its config values.

    log += LogOutput("inputFile",config.fileName,"Input File: ")

    val startTime = System.currentTimeMillis() // Start timer


//    config.train.mapPartitions(x=>JacksonShredder.shred(x))
//      .flatMap(Extract.ExtractAttributes(_))
//      .combineByKey(Extract.createCombiner,Extract.mergeValue,Extract.mergeCombiners)
//      .count()
//

//    val (v,a) = RunExplorer.extractComplexSchemas(config,startTime,log)
//    config.train.mapPartitions(x=>JacksonShredder.shred(x))
//      .flatMap(FeatureVectors.shredJET(v, Set(),_))
//      .combineByKey(x => FeatureVectors.createCombiner(v, Set(),x),FeatureVectors.mergeValue,FeatureVectors.mergeCombiners)
//      .count()
//
//    val eTime = System.currentTimeMillis() // Start timer
//    println(eTime-startTime)
//    ???

    val calculateEntropy = true

    val (variableObjs, objArrs): (Set[AttributeName],Set[AttributeName]) = if(calculateEntropy) {
      RunExplorer.extractComplexSchemas(config,startTime,log)
    } else {
      (Set[AttributeName](),Set[AttributeName]())
    }


    val secondPassStart = System.currentTimeMillis()

    val shreddedRecords: RDD[JsonExplorerType] = RunExplorer.shredRecords(config.train)

    // create feature vectors, currently should work if schemas generated from subset of training data
    val featureVectors: Array[(AttributeName,Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]])] =
      shreddedRecords.flatMap(FeatureVectors.shredJET(variableObjs, objArrs,_))
        .combineByKey(x => FeatureVectors.createCombiner(variableObjs, objArrs,x),FeatureVectors.mergeValue,FeatureVectors.mergeCombiners).collect()

//    FastFeatureVector.extract(variableObjs, objArrs, shreddedRecords)
//      .count()


    val fvTime = System.currentTimeMillis()
    val fvRunTime = fvTime - secondPassStart

    log += LogOutput("FVCreationTime",fvRunTime.toString(),"FV Creation Took: ")

    var algorithmSchema = ""

    if(config.runBiMax.equals(util.CMDLineParser.BiMax)){

      // BiMax algorithm
      val rawSchemas: Map[AttributeName,Types.DisjointNodes] = featureVectors.map(x => {
        x._2 match {
          case Left(l) => (x._1,BiMax.OurBiMax2.bin(l),true)
          case Right(r) =>
            (x._1,
              mutable.Seq(mutable.Seq(
                BiMaxNode(Set[AttributeName](),Map[AttributeName,mutable.Set[JsonExplorerType]](),0,r.map(x => (Map[AttributeName,mutable.Set[JsonExplorerType]]((x._1,x._2._1)),x._2._2)).toList.to[mutable.ListBuffer])
              )), false // don't do bimax on var_objects
            )
        }

      })
        .map(x => if (x._3) (x._1,BiMax.OurBiMax2.rewrite(x._2)) else (x._1,x._2)).toMap

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


      val JsonSchema: util.JsonSchema.JSS = util.NodeToJsonSchema.biMaxToJsonSchema(mergedSchemas,variableObjWithMult, objArrs)
      algorithmSchema = JsonSchema.toString  + "\n"
    } else if(config.runBiMax.equals(util.CMDLineParser.Subset)) {

      // onlySubSet test
      val onlySubset: Map[AttributeName, Types.DisjointNodes] = featureVectors.map(x => {
        x._2 match {
          case Left(l) => (x._1, BiMax.OurBiMax2.bin(l), true)
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
    } else {
      throw new Exception("Unknown Merge algorithm choice")
    }


    val endTime = System.currentTimeMillis() // End Timer


    //log += LogOutput("FVTime",fvRunTime.toString,"FV Creation Took: "," ms")
    log += LogOutput("TotalTime",(endTime - startTime).toString,"Total execution time: ", " ms")
    log += LogOutput("TrainPercent",config.trainPercent.toString,"TrainPercent: ")
    log += LogOutput("ValidationSize",config.validationSize.toString,"ValidationSize: ")
    log += LogOutput("Algorithm",if(config.runBiMax.equals(util.CMDLineParser.BiMax)) "bimax" else if(config.runBiMax.equals(util.CMDLineParser.Subset)) "subset" else if(config.runBiMax.equals(util.CMDLineParser.Verbose)) "verbose" else "unknown","Algorithm: ")
    log += LogOutput("Seed",config.seed match {
      case Some(i) => i.toString
      case None => "None"},"Seed: ")

    config.spark.conf.getAll.foreach{case(k,v) => log += LogOutput(k,v,k+": ")}
    log += LogOutput("kse",config.kse.toString,"KSE: ")

//    println(SizeEstimator.estimate(featureVectors.filter(x=> x._1.isEmpty || attributeMap.get(x._1).get.`type`.contains(JE_Var_Object))))
//    println(SizeEstimator.estimate(featureVectors))



    val logFile = new FileWriter(config.logFileName,true)
    logFile.write("{" + log.map(_.toJson).mkString(",") + "}\n")
    if(config.writeJsonSchema) logFile.write(algorithmSchema)
    logFile.close()
    println(log.map(_.toString).mkString("\n"))

  }

  case class LogOutput(label:String, value:String, printPrefix:String, printSuffix:String = ""){
    override def toString: String = s"""${printPrefix}${value}${printSuffix}"""
    def toJson: String = s""""${label}":"${value}""""
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