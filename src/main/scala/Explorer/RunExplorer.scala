package Explorer

import java.io.{File, PrintWriter}

import Explorer.Types.AttributeName
import JsonExplorer.SparkMain.LogOutput
import Optimizer.RewriteAttributes
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object RunExplorer {

  def shredRecords(input: RDD[String]): RDD[JsonExplorerType] = input.mapPartitions(x=>JacksonShredder.shred(x))

  def presistRecords(shreddedRecords: RDD[JsonExplorerType], storageStrat: Option[Boolean]): Unit = {
    storageStrat match {
      case Some(inmemory) =>
        if(inmemory)
          shreddedRecords.persist(StorageLevel.MEMORY_ONLY)
        else
          shreddedRecords.persist(StorageLevel.DISK_ONLY)
      case None => // don't cache at all
    }
  }

  def extractTypeStructure(shreddedRecords: RDD[JsonExplorerType]): Array[(AttributeName,Attribute)] = {
    return shreddedRecords
      .flatMap(Extract.ExtractAttributes(_))
      .combineByKey(Extract.createCombiner,Extract.mergeValue,Extract.mergeCombiners)
      .map{case(n,t) => {
        (n,Attribute(n,t))
      }}.collect()
  }

  def applyTypeCorrection(extractedAttributes: Array[(AttributeName,Attribute)], kse: Double): AttributeTree = {

    val attributeTree: AttributeTree = RewriteAttributes.attributeListToAttributeTree(extractedAttributes)

    // set objectKeySpaceThreshold to 0.0 to disable var_objects
    // first rewrite arrays to tuples, in case tuple-like-array-of-objects
    RewriteAttributes.checkForTupleArrayOfObjects(attributeTree)
    // now rewrite collections
    RewriteAttributes.rewriteSemanticTypes(attributeTree, kse,0.0,1.0)

    return attributeTree
  }

  def extractComplexSchemas(config: util.CMDLineParser.config, startTime: Long, log: scala.collection.mutable.ListBuffer[LogOutput]): (Set[AttributeName],Set[AttributeName]) = {
    /*
      Shred the input file into JsonExplorerTypes while keeping the JSON tree structure.
      This can then be parsed in the feature vector creation phase without having to re-read the input file.
    */
    val shreddedRecords: RDD[JsonExplorerType] = RunExplorer.shredRecords(config.train)
    /*
      Preform the extraction phase:
        - Traverses the shredded JsonExplorerObject
        - Collects each attributes type information and co-occurrence-lite
        - This can then be converted into key-space and type entropy
     */
    val extractedAttributes: Array[(AttributeName,Attribute)] = RunExplorer.extractTypeStructure(shreddedRecords)
    val extractionTime = System.currentTimeMillis()
    val extractionRunTime = extractionTime - startTime
    log += LogOutput("ExtractionTime", extractionRunTime.toString, "Extraction Time: ")

    val attributeTree: AttributeTree = RunExplorer.applyTypeCorrection(extractedAttributes, config.kse)

    // get schemas to break on
    val schemas: Seq[(AttributeName,JsonExplorerType)] = RewriteAttributes.getSchemas(attributeTree)
      .map(x => (x._1.map(y => if(y.isInstanceOf[Int]) Star else y),x._2)) // remove possibility of weird array stuff
      .toSeq
      .sortBy(_._1.size)(Ordering[Int].reverse)

    val optimizationTime = System.currentTimeMillis()
    val optimizationRunTime = optimizationTime - extractionTime
    log += LogOutput("OptimizationTime", optimizationRunTime.toString(), "Optimization Time: ")

    // TODO check type entropy, might be a bit screwy since it was negative
    val attributeMap = RewriteAttributes.attributeTreeToAttributeMap(attributeTree)

    val variableObjects: Set[AttributeName] = attributeMap.filter(x=> !x._1.isEmpty && attributeMap.get(x._1).get.`type`.contains(JE_Var_Object)).map(_._1)
      .map(x => x.map(y => if(y.isInstanceOf[Int]) Star else y))
      .toSet

    val objArrs: Set[AttributeName] = attributeMap.filter(x=> !x._1.isEmpty && attributeMap.get(x._1).get.`type`.contains(JE_Obj_Array)).map(_._1)
      .map(x => x.map(y => if(y.isInstanceOf[Int]) Star else y))
      .toSet

    variableObjects.foreach(x => if(objArrs.contains(x)) throw new Exception("Array of objects and Variable Object"))

    val RewriteTime = System.currentTimeMillis() // End Timer
    val RewriteRunTime = RewriteTime - optimizationTime
    log += LogOutput("RewriteTime: ", RewriteRunTime.toString(), "Operator Rewrite Time: ")

    if(config.writeEntropy) {
      // write csv of entropy for graphs
      val entropyWriter = new PrintWriter(new File(config.logFileName + ".entropy"))
      entropyWriter.write("name,object_kse,object_te\n")
      entropyWriter.write(attributeMap.map { case (name, attribute) => {
        attribute.objectMarginalKeySpaceEntropy match {
          case Some(v) => List("\"" + Types.nameToString(name) + "\"", attribute.objectMarginalKeySpaceEntropy.get.toString, attribute.objectTypeEntropy.get.toString)
          case None => List()
        }
      }
      }.filter(_.nonEmpty).map(_.mkString(",")).mkString("\n"))
      entropyWriter.close()
    }

    return (variableObjects.filterNot(_.isEmpty),objArrs.filterNot(_.isEmpty))

  }

}
