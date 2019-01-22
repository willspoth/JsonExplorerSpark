package JsonExplorer

import java.awt.{BorderLayout, Color, Dimension, GridLayout}
import java.util

import BiMax.OurBiMax
import Explorer.Types.{AttributeName, SchemaName}
import Explorer._
import org.apache.spark.{SparkConf, SparkContext}
import Optimizer.Planner
import breeze.linalg.{*, DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import NMF.NMFBiCluster_Scala
import Viz.{BiMaxViz, PlannerFrame}
import javax.swing.border.{CompoundBorder, EmptyBorder, LineBorder}
import javax.swing.{JFrame, JPanel}
import org.apache.spark.storage.StorageLevel
import smile.data.AttributeDataset
import smile.plot.{PlotCanvas, PlotPanel, Window}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable




object SparkMain {


  var plannerFrame: PlannerFrame = null

  def main(args: Array[String]) = {


    val(inputFile, memory, useUI, doNMF,spark) = readArgs(args) // Creates the Spark session with its config values.

    val startTime = System.currentTimeMillis() // Start timer

    val records: RDD[String] = spark.textFile(inputFile) // read file

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
    val serializedRecords: RDD[JsonExplorerType] = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{'))) // filter out lines that aren't Json
      .mapPartitions(x => Serializer.serialize(x)) // serialize output
      */
      //.persist(StorageLevel.DISK_ONLY) // persist allows the serialized rdd to exist for the fv creation process

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
          fvs.map(x => FeatureVectorCreator.toDense(x._1,x._2)).map(x => runNMF(x._1,x._2,x._3)).collect()
        case false =>
          val r = fvs.map(x => BiMax.OurBiMax.BiMax(x._1,x._2)).map(x => BiMax.OurBiMax.convertBiMaxNodes(x._1,x._2)).map(x => BiMax.OurBiMax.categorizeAttributes(x._1,x._2)).map(x => BiMax.OurBiMax.buildGraph(x._1,x._2)).collect()
          r.foreach(x => BiMaxViz.viz(root.Schemas.get(x._1).get,x._2))
          println("done")
      }

    val endTime = System.currentTimeMillis() // End Timer
    val FVRunTime = endTime - optimizationTime
    println("Extraction Took: " + extractionRunTime.toString + " ms")
    println("Optimization Took: " + optimizationRunTime.toString + " ms")
    println("FV Creation Took: " + FVRunTime.toString + " ms")
    println("Total execution time: " + (endTime - startTime) + " ms") // Output run time in milliseconds

  }


  // coverage, column order, row order
  def runNMF(name: SchemaName, fvs: DenseMatrix[Double], mults: DenseVector[Double]): (Int,Array[Int],Array[Int]) = {
    //path to data directory
    if(fvs.data.isEmpty)
      return (0,null,null)

    val orig = fvs(*, ::).map( dv => dv.toArray).toArray

    val c = new NMFBiCluster_Scala(fvs, mults)
    //sanity check whether function getFeatureGroupMembershipConfidence works as expected
    //Output of getFeatureGroupMembershipConfidence() is a vector of length K, each i-th (i=1,...,K) value is within [0,1] indicating the membership confidence of feature group i
    //val reconstructed = c.basisVectors * c.projectedMatrix
    val orginal = c.dataMatrix_smile.t
    for (i <- 0 until orginal.cols){
      val vec = orginal(::,i)
      val vecArray = vec.toArray
      //get feature group confidences
      val vec1 = c.getFeatureGroupMembershipConfidence(vecArray)

      val reconstucted = c.basisVectors*vec1
      val diffvec = vec-reconstucted
      val thresh = 0.2
      val aha = diffvec >:> thresh
      val wuhu = diffvec <:< -thresh
      implicit def bool2double(b: Boolean): Double = if (b) 1.0 else 0.0
      val ahaha = aha.map(x => bool2double(x))
      val wuhuhu = wuhu.map(x => bool2double(x))

    }
/*
    val numClusters = 2
    val numIterations = 20
    val clusters = smile.clustering.KMeans.lloyd(orig,numClusters,numIterations)
    val clusterLabels = clusters.getClusterLabel
    val c1 = orig.zip(clusterLabels).sortBy(_._2).map(_._1)

    val d = fvs(*, ::).map( dv => dv.toArray.zip(c.getFeatureOrder()).sortBy(_._2).map(_._1)).toArray.zip(c.getDataOrder()).sortBy(_._2).map(_._1)

    val frame: JFrame = new JFrame(Types.nameToString(name))
    frame.setPreferredSize(new Dimension(3000, 1500))
    frame.setLayout(new GridLayout(1,2))

    val p1 = smile.plot.heatmap(orig)
    val p3 = smile.plot.heatmap(c1)
    val p2 = smile.plot.heatmap(d)

    p1.close
    p2.close
    p3.close

    p1.canvas.setTitle("Original")
    p3.canvas.setTitle("KMeans k=" + numClusters.toString)
    p2.canvas.setTitle("Reshaped")

    frame.getContentPane.add(p1.canvas)
    frame.getContentPane.add(p3.canvas)
    frame.getContentPane.add(p2.canvas)

    p1.canvas.reset()
    p2.canvas.reset()
    p3.canvas.reset()

    frame.pack()
    frame.setVisible(true)

*/

    //(c.getCoverage(),c.getFeatureOrder(),c.getDataOrder())
    (0,c.getFeatureOrder(),c.getDataOrder())
  }

  def readArgs(args: Array[String]): (String,Option[Boolean],Boolean,Boolean,SparkContext) = {
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
    argMap.remove("master")
    argMap.remove("name")
    argMap.foreach(x => conf.set(x._1,x._2))
    val spark: SparkContext = new SparkContext(conf)
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

    (filename, memory, ui, nmf,spark)
  }


}
