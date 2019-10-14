package JsonExplorer

import java.io._
import java.util.Calendar

import Explorer.Types.BiMaxNode
import org.apache.spark.rdd.RDD

//import BiMax.OurBiMax
import Explorer.Types.AttributeName
import Explorer._
import Optimizer.RewriteAttributes
import org.apache.spark.storage.StorageLevel
import util.CMDLineParser
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable


object SparkMain {


  def main(args: Array[String]): Unit = {

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

    val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()
    log += LogOutput("Date",Calendar.getInstance().getTime().toString,"Date: ")

    val config = CMDLineParser.readArgs(args) // Creates the Spark session with its config values.


    log += LogOutput("inputFile",config.fileName,"Input File: ")


    val startTime = System.currentTimeMillis() // Start timer


    /*
      Shred the input file into JsonExplorerTypes while keeping the JSON tree structure.
      This can then be parsed in the feature vector creation phase without having to re-read the input file.
     */
    val shreddedRecords = config.train.mapPartitions(x=>JacksonShredder.shred(x))


//    // for storage comparison
//    config.memory match {
//      case Some(inmemory) =>
//        if(inmemory)
//          shreddedRecords.persist(StorageLevel.MEMORY_ONLY)
//        else
//          shreddedRecords.persist(StorageLevel.DISK_ONLY)
//      case None => // don't cache at all
//    }

    /*
      Preform the extraction phase:
        - Traverses the shredded JsonExplorerObject
        - Collects each attributes type information and co-occurrence-lite
        - This can then be converted into key-space and type entropy
     */

    val extractedAttributes: Array[(AttributeName,Attribute)] = shreddedRecords
      .flatMap(Extract.ExtractAttributes(_))
      .combineByKey(Extract.createCombiner,Extract.mergeValue,Extract.mergeCombiners)
      .map{case(n,t) => {
        (n,Attribute(n,t))
      }}.collect()


    val extractionTime = System.currentTimeMillis()
    val extractionRunTime = extractionTime - startTime

    val attributeTree: AttributeTree = RewriteAttributes.attributeListToAttributeTree(extractedAttributes)

    // set objectKeySpaceThreshold to 0.0 to disable var_objects
    RewriteAttributes.rewriteSemanticTypes(attributeTree, config.kse,0.0,1.0)

    // TODO check type entropy, might be a bit screwy since it was negative
    // get schemas to break on
    val schemas: Seq[(AttributeName,JsonExplorerType)] = RewriteAttributes.getSchemas(attributeTree)
      .map(x => (x._1.map(y => if(y.isInstanceOf[Int]) Star else y),x._2)) // remove possibility of weird array stuff
      .toSeq
      .sortBy(_._1.size)(Ordering[Int].reverse)

    val optimizationTime = System.currentTimeMillis()
    val optimizationRunTime = optimizationTime - extractionTime

    val attributeMap = RewriteAttributes.attributeTreeToAttributeMap(attributeTree)

    val variableObjects: Set[AttributeName] = attributeMap.filter(x=> !x._1.isEmpty && attributeMap.get(x._1).get.`type`.contains(JE_Var_Object)).map(_._1).toSet

    val RewriteTime = System.currentTimeMillis() // End Timer
    val RewriteRunTime = RewriteTime - extractionRunTime

    // create feature vectors, currently should work if schemas generated from subset of training data
    val featureVectors: RDD[(AttributeName,Either[mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int],mutable.HashMap[AttributeName,(mutable.Set[JsonExplorerType],Int)]])] =
      shreddedRecords.flatMap(FeatureVectors.create(schemas.map(_._1).filterNot(_.isEmpty),_))
        .combineByKey(x => FeatureVectors.createCombiner(variableObjects,x),FeatureVectors.mergeValue,FeatureVectors.mergeCombiners)


    val fvTime = System.currentTimeMillis()
    val fvRunTime = fvTime - optimizationTime

    var algorithmSchema = ""

    if(config.runBiMax){

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
        .map(x => if (x._3) (x._1,BiMax.OurBiMax2.rewrite(x._2)) else (x._1,x._2)).collect().toMap

      val JsonSchema: util.JsonSchema.JSS = util.NodeToJsonSchema.biMaxToJsonSchema(rawSchemas, attributeMap)
      algorithmSchema = JsonSchema.toString  + "\n"
    } else {

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
        .map(x => (x._1, x._2)).collect().toMap

      algorithmSchema = util.NodeToJsonSchema.biMaxToJsonSchema(onlySubset, attributeMap).toString  + "\n"
    }

    val endTime = System.currentTimeMillis() // End Timer


    log += LogOutput("ExtractionTime",extractionRunTime.toString,"Extraction Took: "," ms")
    log += LogOutput("OptimizationTime",optimizationRunTime.toString,"Optimization Took: "," ms")
    //log += LogOutput("FVTime",fvRunTime.toString,"FV Creation Took: "," ms")
    log += LogOutput("TotalTime",(endTime - startTime).toString,"Total execution time: ", " ms")
    log += LogOutput("TrainPercent",config.trainPercent.toString,"TrainPercent: ")
    log += LogOutput("ValidationSize",config.validationSize.toString,"ValidationSize: ")
    log += LogOutput("Algorithm",if(config.runBiMax) "BiMax" else "Subset","Algorithm: ")
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
