package JsonExplorer

import Explorer._
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import NMF.NMFBiCluster_Scala
import Optimizer.Planner
import breeze.linalg.norm
import org.apache.spark.rdd.RDD


object SparkMain {

  //-Xmx6g

  def main(args: Array[String]) = {

    val startTime = System.currentTimeMillis() // Start timer

    // Creates the Spark session with its config values.
    val conf = new SparkConf().set("spark.driver.maxResultSize", "4g").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
      .setMaster("local[*]").setAppName("JSON Explorer")
    val spark = new SparkContext(conf)

    // global parameters, will go away soon
    val pathToFeatureVectors = "C:\\Users\\Will\\Documents\\GitHub\\JsonExplorerSpark\\"

    // read file passed as commandline arg
    val inputLocation: String = args(0)
    val records: RDD[String] = spark.textFile(inputLocation)

    /*
      Serialize the input file into JsonExplorerTypes while keeping the JSON tree structure.
      This can then be parsed in the feature vector creation phase without having to re-read the input file.
     */
    val serializedRecords: RDD[JsonExplorerType] = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{'))) // filter out lines that aren't Json
      .mapPartitions(x => Serializer.serialize(x)) // serialize output
      //.cache()

    /*
      Preform the extraction phase:
        - Traverses the serialized JsonExplorerObject
        - Collects each attributes type information and co-occurrence-lite
        - This can then be converted into key-space and type entropy
     */
    val root: JsonExtractionRoot = serializedRecords
      .mapPartitions(x => Extract.ExtractAttributes(x)).reduce(Extract.combineAllRoots(_,_)) // extraction phase


    // compute entropy and reassemble tree for optimizations
    val kse_intervals = Planner.buildOperatorTree(root) // updates root by reference

    // finds the largest interval, this will be the starting kse
    val kse_threshold = Planner.inferKSE(kse_intervals.sortBy(_._2))

    val operatorConverter = new Optimizer.ConvertOperatorTree(root)
    operatorConverter.Rewrite(kse_threshold) // put in loop and visualize
    operatorConverter.Keep()


    // create feature vectors from this list

    val fvs = serializedRecords.flatMap(FeatureVectorCreator.extractFVSs(root.Schemas,_)).map(x=>{((x.parentName,x.libsvm),1)})
      .reduceByKey(_+_).collect()

    val fvDir = "FeatureVectors/"
    val schemaWriters: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],(BufferedWriter,BufferedWriter)] = root.Schemas.map{case(name,schema)=>{
      val stringName = fvDir + Types.nameToFileString(name)
      val schWriter = new BufferedWriter(new FileWriter(new File(stringName+".sch")))
      schWriter.write(schema.attributes.map(x=>Types.nameToString(x._1)).mkString(","))
      schWriter.close()

      val fvWriter = new BufferedWriter(new FileWriter(new File(stringName+".features")))
      val multWriter = new BufferedWriter(new FileWriter(new File(stringName+".mults")))
      (name,(fvWriter,multWriter))
    }}

    fvs.foreach{case((name,fv),mult)=>{
      val (fvWriter,multWriter) = schemaWriters.get(name).get
      fvWriter.write(fv+'\n')
      multWriter.write(mult.toString+'\n')
    }}

    schemaWriters.foreach(x=> {
      x._2._1.close()
      x._2._2.close()
    })

    root.Schemas.foreach{case(name,schema)=>{
      val stringName = fvDir + Types.nameToFileString(name)
      if((new java.io.File(pathToFeatureVectors+stringName+".mults")).length()>0) {
        println("running NMF on: " + stringName)
        runNMF(pathToFeatureVectors, stringName)
      }
    }}

    // write fvs
    //fvs.foreach()

    // run nmf and display results


    val endTime = System.currentTimeMillis() // End Timer
    println("Total execution time: " + (endTime - startTime) + " ms") // Output run time in milliseconds

  }


  def runNMF(pathToFeatureVectorFolder:String, attr:String): Unit = {

    val c = new NMFBiCluster_Scala(pathToFeatureVectorFolder + attr + ".features", pathToFeatureVectorFolder + attr + ".mults")

    //sanity check whether function getFeatureGroupMembershipConfidence works as expected
    //Output of getFeatureGroupMembershipConfidence() is a vector of length K, each i-th (i=1,...,K) value is within [0,1] indicating the membership confidence of feature group i
    val reconstructed = c.basisVectors * c.projectedMatrix
    for (i <- 0 until reconstructed.cols) {
      val vec = reconstructed(::, i)
      //get feature group confidences
      val vec1 = c.getFeatureGroupMembershipConfidence(vec.toArray)
      //get feature vectors of original data matrix that have been projected onto basis vectors
      val vec2 = c.projectedMatrix(::, i)
      //group confidences should align with projected rows of the matrix
      val diff = norm(vec1 - vec2)
      if (diff > 1E-13)
        println(diff)
    }
    //check if functions that gets the re-ordering of features and data tuples work without error
    val indACluster = c.getFeatureOrder()
    val indYCluster = c.getDataOrder()
  }


}
