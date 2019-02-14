package JsonExplorer

import java.io._

import Explorer.Types.{AttributeName, SchemaName}
import Explorer._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import Viz.{BiMaxViz, PlannerFrame}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object SparkMain {


  var plannerFrame: PlannerFrame = null

  def main(args: Array[String]) = {

    /*
    val mas = scala.collection.mutable.HashMap[(ListBuffer[Any],(scala.collection.mutable.HashSet[Int],scala.collection.mutable.HashSet[Int])),Int]()
    val l = scala.collection.mutable.HashSet[Int](1,2,3)
    val ro = ListBuffer[Any]()
    val lb1 = ListBuffer[Any]("test","one")
    val lb2 = ListBuffer[Any]("test","two")
    val r = scala.collection.mutable.HashSet[Int]()
    mas.put((lb1,(l,r)),1)
    println(mas.contains((ro,(l,r))))
    println(mas.contains((lb1,(l,r))))
    println(mas.contains((lb2,(l,r))))


    ???
    */

    val(inputFile, memory, useUI, doNMF,spark,name) = readArgs(args) // Creates the Spark session with its config values.

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

    val records: RDD[String] = spark.sparkContext.textFile(inputFile) // read file

    /*
      Serialize the input file into JsonExplorerTypes while keeping the JSON tree structure.
      This can then be parsed in the feature vector creation phase without having to re-read the input file.
     */
    val serializedRecords = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{')))
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
    println("Extraction Took: " + extractionRunTime.toString + " ms")

    val root: JsonExtractionRoot = new JsonExtractionRoot()
    root.AllAttributes = scala.collection.mutable.HashMap(extracted: _*)

    plannerFrame = new PlannerFrame(root,useUI) // root is updated in here


    val optimizationTime = System.currentTimeMillis()
    val optimizationRunTime = optimizationTime - extractionTime
    println("Optimization Took: " + optimizationRunTime.toString + " ms")
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
          Viz.BiMaxViz.viz(name,root,g,l)
      }

    val endTime = System.currentTimeMillis() // End Timer
    val FVRunTime = endTime - optimizationTime
    println("Extraction Took: " + extractionRunTime.toString + " ms")
    println("Optimization Took: " + optimizationRunTime.toString + " ms")
    println("FV Creation Took: " + FVRunTime.toString + " ms")
    println("Total execution time: " + (endTime - startTime) + " ms") // Output run time in milliseconds

  }


  def readArgs(args: Array[String]): (String,Option[Boolean],Boolean,Boolean,SparkSession,String) = {
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
      case Some("memory" | "inmemory" | "true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false" | "disk") => false
      case _ | None => false
    }

    (filename, memory, ui, nmf,spark,name)
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

}
