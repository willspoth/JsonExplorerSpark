package JsonExplorer

import java.io._
import java.util.Calendar

import BiMax.OurBiMax
import Explorer.Types.{AttributeName, SchemaName}
import Explorer._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import Viz.PlannerFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object SparkMain {


  var plannerFrame: PlannerFrame = null

  def main(args: Array[String]): Unit = {


    val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()

    log += LogOutput("Date",Calendar.getInstance().getTime().toString,"Date: ")

    val(inputFile, memory, useUI, doNMF,spark,name,outputLog) = readArgs(args) // Creates the Spark session with its config values.

    """
    val j = spark.read.json(inputFile)
    var attributes: Int = 0
    //val s = new StructField(null,null,null).getClass
    def countAttr(f: StructField): Unit = {
      f.dataType match {
        case f2: StructType => f2.fields.foreach(countAttr(_))
        case arr: ArrayType => arr.elementType
        case _ => attributes += 1
      }
    }
    j.printSchema()
    j.schema.toList.foreach(countAttr(_))
    println(attributes)
    """



    val startTime = System.currentTimeMillis() // Start timer

    val data: Array[RDD[String]] = spark.sparkContext.textFile(inputFile).filter(x => (x.size > 0 && x.charAt(0).equals('{')))
      .randomSplit(Array[Double](0.1,0.9)) // read file
    val test: RDD[String] = data.head
    val validation: RDD[String] = data.last
    log += LogOutput("TestSize",test.count().toString,"TestSize: ")
    log += LogOutput("ValidationSize",validation.count().toString,"ValidationSize: ")

    /*
      Serialize the input file into JsonExplorerTypes while keeping the JSON tree structure.
      This can then be parsed in the feature vector creation phase without having to re-read the input file.
     */
    val serializedRecords = test
      .mapPartitions(x=>JacksonSerializer.serialize(x))
    memory match {
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

    plannerFrame = new PlannerFrame(root,useUI) // root is updated in here


    val optimizationTime = System.currentTimeMillis()
    val optimizationRunTime = optimizationTime - extractionTime
    //println("Optimization Took: " + optimizationRunTime.toString + " ms")
    // create feature vectors from this list

    val fvs = serializedRecords.flatMap(FeatureVectorCreator.extractFVSs(root.Schemas,_))
      .combineByKey(FeatureVectorCreator.createCombiner,FeatureVectorCreator.mergeValue,FeatureVectorCreator.mergeCombiners)
      //.reduceByKey(FeatureVectorCreator.Combine(_,_)).map(x => FeatureVectorCreator.toDense(x._1,x._2))
      doNMF match {
        case true =>
          fvs.map(x => FeatureVectorCreator.toDense(x._1,x._2)).map(x => NMF.RunNMF.runNMF(root.Schemas.get(x._1).get,x._2,x._3)).collect()
        case false =>
          val r = fvs.map(x => BiMax.OurBiMax.BiMax(x._1,x._2)).map(x => BiMax.OurBiMax.convertBiMaxNodes(x._1,x._2)).map(x => BiMax.OurBiMax.categorizeAttributes(x._1,x._2)).collect()
          val (g,l) = BiMax.OurBiMax.buildGraph(root,r)
          log += LogOutput("Precision",BiMax.OurBiMax.calculatePrecision(ListBuffer[Any](),g,l).toString(),"Precision: ")
          val schemaSet = OurBiMax.graphToSchemaSet(root,g,l)
          log += LogOutput("Validation",((validation.mapPartitions(x => JacksonSerializer.serialize(x)).map(x => OurBiMax.splitForValidation(x)).map(x => BiMax.OurBiMax.calculateValidation(x,schemaSet)).reduce(_+_)/validation.count().toDouble)*100.0).toString(),"Validation: ","%")
          Viz.BiMaxViz.viz(name,root,g,l)

      }

    val endTime = System.currentTimeMillis() // End Timer
    val FVRunTime = endTime - optimizationTime
    log += LogOutput("ExtractionTime",extractionRunTime.toString,"Extraction Took: "," ms")
    log += LogOutput("OptimizationTime",optimizationRunTime.toString,"Optimization Took: "," ms")
    log += LogOutput("FVCreationTime",FVRunTime.toString,"FV Creation Took: "," ms")
    log += LogOutput("TotalTime",(endTime - startTime).toString,"Total execution time: ", " ms")
    if(outputLog) // write out log
      new PrintWriter("log.json") { append("{"+log.map(_.toJson).mkString(",")+"}"); close }
    else
      println(log.map(_.toString).mkString("\n"))
  }


  def readArgs(args: Array[String]): (String,Option[Boolean],Boolean,Boolean,SparkSession,String,Boolean) = {
    if(args.size == 0 || args.size%2 == 0) {
      println("Unexpected Argument, should be, filename -master xxx -name xxx -sparkinfo xxx -sparkinfo xxx")
      System.exit(0)
    }
    val argMap = scala.collection.mutable.HashMap[String,String]("master"->"local[*]","name"->"JsonExplorer")
    val filename: String = args(0)
    if(args.tail.size > 1) {
      val argPairs = args.tail.zip(args.tail.tail).zipWithIndex.filter(_._2%2==0).map(_._1).foreach(x=>argMap.put(x._1.tail,x._2))
    }
    val conf = new SparkConf().setMaster(argMap.get("master").get).setAppName(argMap.get("name").get)
    val name = argMap.get("name").get
    argMap.remove("master")
    argMap.remove("name")
    argMap.foreach(x => conf.set(x._1,x._2))

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.config(conf).getOrCreate()
    val memory: Option[Boolean] = argMap.get("memory") match {
      case Some("memory" | "inmemory" | "true" | "t" | "y" | "yes") => Some(true)
      case Some("n" | "no" | "false" | "disk") => Some(false)
      case _ | None => None
    }

    val ui: Boolean = argMap.get("ui") match {
      case Some("memory" | "inmemory" | "true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false" | "disk") => false
      case _ | None => false
    }

    val nmf: Boolean = argMap.get("nmf") match {
      case Some("true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false") => false
      case _ | None => false
    }

    val outputLog: Boolean = argMap.get("log") match {
      case Some("true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false") => false
      case _ | None => false
    }

    (filename, memory, ui, nmf,spark,name,outputLog)
  }

  // these are special parsers for our data just to get things running, will replace with better solution for recall tests

  def github(): Unit = {
    val file = new File("C:\\Users\\Will\\Desktop\\JsonData\\github1m.json")
    val bw = new BufferedWriter(new FileWriter(file))
    for( a <- 1 to 100){
      bw.write(scala.io.Source.fromFile("C:\\Users\\Will\\Desktop\\JsonData\\github\\github"+a.toString+".json").mkString+'\n')
      println(a)
    }
    bw.close()
    ???
  }


  def clean(inputName: String, rowLimit: Int = 0): Unit = {
    val in = new File(inputName)
    val br = new BufferedReader(new FileReader(in))
    val out = new File(inputName+"out")
    val bw = new BufferedWriter(new FileWriter(out))
    var rowCount: Int = 0
    var wrongCount: Int = 0
    var line: String = null
    var break: Boolean = false
    while(((rowCount < rowLimit) || (rowLimit == 0)) && ((line = br.readLine())!= null) && !break){
      if(line != null && line.size > 1 && (line.charAt(0).equals('{') && line.charAt(line.size-1).equals('}'))) {
        bw.write(line + '\n')
        rowCount += 1
        if(rowCount % 100000 == 0)
          println(rowCount.toString + " rows so far" + wrongCount.toString + " wrong lines found")
      } else {
        wrongCount += 1
        if(wrongCount%100 == 0) {
          break = true
          System.err.println(wrongCount.toString + " wrong lines found")
        }
      }
    }
    println(rowCount)
    br.close()
    bw.close()
    ???
  }

  case class LogOutput(label:String, value:String, printPrefix:String, printSuffix:String = ""){
    override def toString: String = s"""${printPrefix}${value}${printSuffix}"""
    def toJson: String = s""""${label}":"${value}""""
  }


}
