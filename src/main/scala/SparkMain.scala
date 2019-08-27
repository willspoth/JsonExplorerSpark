package JsonExplorer

import java.io._
import java.util.Calendar

import BiMax.OurBiMax
import Explorer.Types.{AttributeName, SchemaName}
import Explorer._
import Naive.{Flat, Verbose}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import Viz.PlannerFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import util.CMDLineParser

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object SparkMain {


  var plannerFrame: PlannerFrame = null

  def main(args: Array[String]): Unit = {

    CMDLineParser.createSparkSession(Some("/home/will/projects/JsonExplorer/src/main/resources/spark.conf"))
    val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()

    log += LogOutput("Date",Calendar.getInstance().getTime().toString,"Date: ")

    val config = CMDLineParser.readArgs(args) // Creates the Spark session with its config values.


    //spark.read.json(inputFile).schema.printTreeString()
    //???

    val startTime = System.currentTimeMillis() // Start timer

    val totalNumberOfLines: Long = config.spark.sparkContext.textFile(config.fileName).filter(x => (x.size > 0 && x.charAt(0).equals('{'))).count()
    var trainSize: Double = totalNumberOfLines.toDouble*(config.trainPercentage/100.0)
    if(config.trainPercentage > 100.0)
      throw new Exception("Test Percent can't be higher than 100%, Found: " + config.trainPercentage.toString)
    else if((trainSize + config.validationSize) > totalNumberOfLines) {
      trainSize = totalNumberOfLines.toDouble - config.validationSize.toDouble
      println("Total Percent can't be higher than 100%, Found: " + config.trainPercentage.toString + " + " + (config.validationSize.toDouble/totalNumberOfLines.toDouble).toString + " setting test Percent to " +config.trainPercentage.toString)
    }
    val overflow: Double = totalNumberOfLines.toDouble - config.validationSize.toDouble - trainSize.toDouble
    val data: Array[RDD[String]] = config.spark.sparkContext.textFile(config.fileName).filter(x => (x.size > 0 && x.charAt(0).equals('{')))
      .randomSplit(Array[Double](trainSize,config.validationSize.toDouble,overflow),seed = 1000) // read file
    val train: RDD[String] = data.head
    val validation: RDD[String] = data(1)
    log += LogOutput("TestSize",train.count().toString,"TestSize: ")
    log += LogOutput("ValidationSize",validation.count().toString,"ValidationSize: ")

    /*
      Serialize the input file into JsonExplorerTypes while keeping the JSON tree structure.
      This can then be parsed in the feature vector creation phase without having to re-read the input file.
     */
    val serializedRecords = train.mapPartitions(x=>JacksonSerializer.serialize(x))
    config.memory match {
      case Some(inmemory) =>
        if(inmemory)
          serializedRecords.persist(StorageLevel.MEMORY_ONLY)
        else
          serializedRecords.persist(StorageLevel.DISK_ONLY)
      case None => // don't cache at all
    }


    /*
      Preform the extraction phase:
        - Traverses the serialized JsonExplorerObject
        - Collects each attributes type information and co-occurrence-lite
        - This can then be converted into key-space and type entropy
     */

    val extracted: Array[(AttributeName,Attribute)] = serializedRecords.flatMap(Extract.ExtractAttributes(_)).combineByKey(Extract.createCombiner,Extract.mergeValue,Extract.mergeCombiners)
      .map{case(n,t) => {
        val a = Attribute()
        a.name = n
        a.types = t
        (n,a)
      }}.collect()
    val extractionTime = System.currentTimeMillis()
    val extractionRunTime = extractionTime - startTime
    //println("Extraction Took: " + extractionRunTime.toString + " ms")

    val root: JsonExtractionRoot = new JsonExtractionRoot()
    root.AllAttributes = scala.collection.mutable.HashMap(extracted: _*)

    plannerFrame = new PlannerFrame(root,config.useUI) // root is updated in here


    val optimizationTime = System.currentTimeMillis()
    val optimizationRunTime = optimizationTime - extractionTime
    //println("Optimization Took: " + optimizationRunTime.toString + " ms")
    // create feature vectors from this list
    var (orig,kmeans,bimax): (Array[Array[Double]],Array[Array[Double]],Array[Array[Double]]) = (null,null,null)
    var bimaxSchema: ListBuffer[(mutable.HashSet[Types.AttributeName], mutable.HashSet[Types.AttributeName])]  = null

    val fvs = serializedRecords.flatMap(FeatureVectorCreator.extractFVSs(root.Schemas,_))
      .combineByKey(FeatureVectorCreator.createCombiner,FeatureVectorCreator.mergeValue,FeatureVectorCreator.mergeCombiners)
      //.reduceByKey(FeatureVectorCreator.Combine(_,_)).map(x => FeatureVectorCreator.toDense(x._1,x._2))

      val r = fvs.map(x => BiMax.OurBiMax.BiMax(x._1,x._2)).map(x => BiMax.OurBiMax.convertBiMaxNodes(x._1,x._2)).map(x => BiMax.OurBiMax.categorizeAttributes(x._1,x._2)).collect()
      val (g,l) = BiMax.OurBiMax.buildGraph(root,r)
      log += LogOutput("Precision",BiMax.OurBiMax.calculatePrecision(ListBuffer[Any](),g,l).toString(),"Precision: ")
      val schemaSet = OurBiMax.graphToSchemaSet(root,g,l)
      bimaxSchema = schemaSet.map { case (m, o) => Tuple2(mutable.HashSet[AttributeName](), m ++ o) }
      log += LogOutput("Grouping",schemaSet.size.toString(),"Number of Groups: ")

      if(config.validationSize > 0) {
        val vali = validation.mapPartitions(x => JacksonSerializer.serialize(x)).map(x => OurBiMax.splitForValidation(x)).map(x => BiMax.OurBiMax.calculateValidation(x, schemaSet.map { case (m, o) => Tuple2(mutable.HashSet[AttributeName](), m ++ o) })).reduce(_ + _)
        log += LogOutput("Validation", ((vali / validation.count().toDouble) * 100.0).toString(), "Validation: ", "%")
      }
      if(config.generateDot)
        Viz.BiMaxViz.viz(config.name,root,g,l)


    val endTime = System.currentTimeMillis() // End Timer
    val FVRunTime = endTime - optimizationTime
    log += LogOutput("ExtractionTime",extractionRunTime.toString,"Extraction Took: "," ms")
    log += LogOutput("OptimizationTime",optimizationRunTime.toString,"Optimization Took: "," ms")
    log += LogOutput("FVCreationTime",FVRunTime.toString,"FV Creation Took: "," ms")
    log += LogOutput("TotalTime",(endTime - startTime).toString,"Total execution time: ", " ms")


    // run naive implementation
    if(false){ // naive flat comparison
      Flat.test(train,validation,log,config.generateDot)
      Verbose.test(train,validation,log,config.generateDot)
      val temp = Naive.KMeans.test(train,validation,log,bimaxSchema,config.generateDot, config.k)
      orig=temp._1;kmeans=temp._2;bimax=temp._3
      //println(log.map(_.toString).mkString("\n"))
    }

    val logFile = new FileWriter("log.json",true)
    logFile.write("{" + log.map(_.toJson).mkString(",") + "}\n")
    logFile.close()
    println(log.map(_.toString).mkString("\n"))

    if(false && (orig != null && kmeans != null && bimax != null))
      Viz.KMeansViz.viz(orig,kmeans,bimax)

  }

  case class LogOutput(label:String, value:String, printPrefix:String, printSuffix:String = ""){
    override def toString: String = s"""${printPrefix}${value}${printSuffix}"""
    def toJson: String = s""""${label}":"${value}""""
  }


}
