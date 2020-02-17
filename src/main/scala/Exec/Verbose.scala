package Exec

import Explorer.Types.AttributeName
import Explorer.{Attribute, Extract, JE_Boolean, JE_Numeric, JE_String, JacksonShredder, JsonExplorerType}
import JsonExplorer.SparkMain.LogOutput
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Verbose {

  private def shredRecords(input: RDD[String]): RDD[JsonExplorerType] =
    input.mapPartitions(x=>JacksonShredder.shred(x))

  private def extractTypeStructure(shreddedRecords: RDD[JsonExplorerType]): Set[scala.collection.mutable.HashMap[AttributeName,scala.collection.mutable.Set[JsonExplorerType]]] =
    shreddedRecords.map(Extract.ExtractCombinedAttributes(_)).distinct().collect().toSet

  private def validateRows(schemas: Set[scala.collection.mutable.HashMap[AttributeName,scala.collection.mutable.Set[JsonExplorerType]]], validationSet: RDD[String]): Option[Long] = {
    val validationSetSize = validationSet.count()
    if(validationSetSize > 0)
      return Some(shredRecords(validationSet).map(Extract.ExtractCombinedAttributes(_)).map(x => if(schemas.contains(x)) 1 else 0).reduce(_+_) / validationSet.count())
    else return None
  }


  def run(train: RDD[String], validate: RDD[String], log: mutable.ListBuffer[LogOutput]): Unit = {
    val schemas = extractTypeStructure(shredRecords(train)) // issue with multiple different types and nulls
    //LogOutput("Precision", validateRows(schemas, validate).toString(), "Precision: ")
    LogOutput("Recall", validateRows(schemas, validate).toString(), "Recall: ")
    LogOutput("Grouping", schemas.size.toString(), "Grouping: ")
  }

}
