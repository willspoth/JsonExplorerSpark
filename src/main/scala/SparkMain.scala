package JsonExplorer

import java.awt.{BorderLayout, Color, Dimension, GridLayout}

import Explorer._
import org.apache.spark.{SparkConf, SparkContext}
import Optimizer.Planner
import breeze.linalg.{*, DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import NMF.NMFBiCluster_Scala
import javax.swing.border.{CompoundBorder, EmptyBorder, LineBorder}
import javax.swing.{JFrame, JPanel}
import org.apache.spark.storage.StorageLevel
import smile.plot.{PlotCanvas, PlotPanel, Window}

import scala.collection.mutable.ListBuffer




object SparkMain {

  //-Xmx6g
  //C:\Users\Will\Documents\GitHub\JsonExplorer\clean\yelp10000.merged -master local[*] -name hello -spark.driver.maxResultSize 4g -spark.driver.memory 4g -spark.executor.memory 4g
  //C:\Users\William\Desktop\Data\jsonData\yelp.json
  //C:/Users/William/Documents/Github/JsonExplorerSpark/data/yelp10000.merged

  def main(args: Array[String]) = {


    val(inputFile, spark) = readArgs(args)

    val startTime = System.currentTimeMillis() // Start timer

    // Creates the Spark session with its config values.


    // read file passed as commandline arg
    val records: RDD[String] = spark.textFile(inputFile)

    /*
      Serialize the input file into JsonExplorerTypes while keeping the JSON tree structure.
      This can then be parsed in the feature vector creation phase without having to re-read the input file.
     */
    val serializedRecords = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{')))
      .mapPartitions(x=>JacksonSerializer.serialize(x))
    /*
    val serializedRecords: RDD[JsonExplorerType] = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{'))) // filter out lines that aren't Json
      .mapPartitions(x => Serializer.serialize(x)) // serialize output
      */
      .persist(StorageLevel.DISK_ONLY)

    /*
      Preform the extraction phase:
        - Traverses the serialized JsonExplorerObject
        - Collects each attributes type information and co-occurrence-lite
        - This can then be converted into key-space and type entropy
     */
    val root: JsonExtractionRoot = serializedRecords
      .mapPartitions(x => Extract.ExtractAttributes(x)).reduce(Extract.combineAllRoots(_,_)) // extraction phase


    val extractionTime = System.currentTimeMillis()
    val extractionRunTime = extractionTime - startTime
    println("Extraction Took: " + extractionRunTime.toString + " ms")

    // compute entropy and reassemble tree for optimizations
    val kse_intervals = Planner.buildOperatorTree(root) // updates root by reference

    // finds the largest interval, this will be the starting kse
    val kse_threshold = Planner.inferKSE(kse_intervals.sortBy(_._2))
    Planner.setNaiveTypes(root)

/*
    val m: Array[Array[Double]] = kse_intervals.map(x => {
      root.AllAttributes.get(x._1).get.naiveType.getType() match {
        case JE_Array | JE_Empty_Array | JE_Obj_Array => List(x._2,1.0).toArray
        case JE_Object | JE_Empty_Object | JE_Var_Object => List(x._2,0.0).toArray
        case _ => List(x._2,-1.0).toArray
      }
    }).toArray

    val entropyChart = smile.plot.plot(m)
    entropyChart.close
*/

//    val temp = root.AllAttributes.get(ListBuffer[Any]("attributes"))

    val operatorConverter = new Optimizer.ConvertOperatorTree(root)
    operatorConverter.Rewrite(kse_threshold) // put in loop and visualize
/*
    val frame: JFrame = new JFrame()
    val (tree,depth) = Types.buildNodeTree(operatorConverter.allAttributes)

    val panel: Viz.DrawTree = new Viz.DrawTree(tree,depth,operatorConverter.allAttributes)

    frame.setSize(2000,1500)

    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.setLayout(new GridLayout(1,2))
    frame.setVisible(true)

    frame.getContentPane.add(panel)
    frame.getContentPane.add(entropyChart.canvas)



    while(true){
      val donothing = true
    }
    */
    operatorConverter.Keep()

    val optimizationTime = System.currentTimeMillis()
    val optimizationRunTime = optimizationTime - extractionTime
    println("Optimization Took: " + optimizationRunTime.toString + " ms")
    // create feature vectors from this list

    val fvs = serializedRecords.flatMap(FeatureVectorCreator.extractFVSs(root.Schemas,_))
      .reduceByKey(FeatureVectorCreator.Combine(_,_)).map(x => FeatureVectorCreator.toDense(x._1,x._2))
      //.map(x => runNMF(x._1,x._2,x._3))
      .collect()


    val endTime = System.currentTimeMillis() // End Timer
    val FVRunTime = endTime - optimizationTime
    println("Extraction Took: " + extractionRunTime.toString + " ms")
    println("Optimization Took: " + optimizationRunTime.toString + " ms")
    println("FV Creation Took: " + FVRunTime.toString + " ms")
    println("Total execution time: " + (endTime - startTime) + " ms") // Output run time in milliseconds

  }


  // coverage, column order, row order
  def runNMF(name: scala.collection.mutable.ListBuffer[Any], fvs: DenseMatrix[Double], mults: DenseVector[Double]): (Int,Array[Int],Array[Int]) = {
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



    //(c.getCoverage(),c.getFeatureOrder(),c.getDataOrder())
    (0,c.getFeatureOrder(),c.getDataOrder())
  }

  def readArgs(args: Array[String]): (String,SparkContext) = {
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
    (filename,spark)
  }


}
