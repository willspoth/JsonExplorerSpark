package Exec


import Explorer.Attribute
import Explorer.Types.AttributeName
import Explorer.{AttributeTree, Extract, JacksonShredder, JsonExplorerType}
import Optimizer.RewriteAttributes
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object BiMaxExplorer {

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

}
