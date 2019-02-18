package Naive

import BiMax.OurBiMax
import Explorer.JacksonSerializer
import Explorer.Types.AttributeName
import JsonExplorer.SparkMain.LogOutput
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Verbose {
  def test(train: RDD[String], validation: RDD[String], log: mutable.ListBuffer[LogOutput]): Unit = {
    val verboseRows: ListBuffer[(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])] = train.mapPartitions(JacksonSerializer.serialize(_)).map(BiMax.OurBiMax.splitForValidation(_)).aggregate(scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]]())(mergeValue,mergeCombiners)
      .map(x => Tuple2(x,scala.collection.mutable.HashSet[AttributeName]())).toList.to[ListBuffer]
    // calculate Precision
    log += LogOutput("VerbosePrecision",verboseRows.size.toString(),"Verbose Precision: ")
    // calculate Validation
    val vali = validation.mapPartitions(x => JacksonSerializer.serialize(x)).map(x => OurBiMax.splitForValidation(x)).map(x => BiMax.OurBiMax.calculateValidation(x,verboseRows)).reduce(_+_)
    log += LogOutput("VerboseValidation",((vali/validation.count().toDouble)*100.0).toString(),"Verbose Validation: ","%")
  }

  def mergeValue(s: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]], row: scala.collection.mutable.HashSet[AttributeName]): scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]] = {
    s += row
    s
  }

  def mergeCombiners(s1: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]], s2: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]]): scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]] = {
    s1 ++= s2
  }
}
