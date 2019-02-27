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
    makeGraph(verboseRows)
    // calculate Precision
    log += LogOutput("VerbosePrecision",verboseRows.size.toString(),"Verbose Precision: ")
    // calculate Validation
    if(validation.count() > 0) {
      val vali = validation.mapPartitions(x => JacksonSerializer.serialize(x))
        .map(x => OurBiMax.splitForValidation(x))
        .map(x => BiMax.OurBiMax.calculateValidation(x, verboseRows))
        .reduce(_ + _)
      log += LogOutput("VerboseValidation", ((vali / validation.count().toDouble) * 100.0).toString(), "Verbose Validation: ", "%")
    }
  }

  private def mergeValue(s: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]], row: scala.collection.mutable.HashSet[AttributeName]): scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]] = {
    s += row
    s
  }

  private def mergeCombiners(s1: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]], s2: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]]): scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]] = {
    s1 ++= s2
  }

  private def makeGraph(schemas: ListBuffer[(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])]): Unit = {
    schemas.zipWithIndex.filter(x=> x._2 < 6 || x._2 == 175)
      .foreach(x => Flat.makeDot(Flat.makeGraph(x._1._1++x._1._2),"VerboseDots/",x._2.toString))
    //((schemas(1)._1++schemas(1)._1).union((schemas(2)._1++schemas(2)._1)) -- (schemas(1)._1++schemas(1)._1).intersect((schemas(2)._1++schemas(2)._1))).foreach(x => println(Explorer.Types.nameToString(x)))
    val s1 = (schemas(1)._1++schemas(1)._1)
    val s2 = (schemas(2)._1++schemas(2)._1)
    //(s1--s2).foreach(x => println(Explorer.Types.nameToString(x)))
    //println("---------------------------------------------------------------------------")
    //(s2--s1).foreach(x => println(Explorer.Types.nameToString(x)))
    schemas.foreach(s => {
      val sp = s._1++s._2
      if(s1.subsetOf(sp) && s2.subsetOf(sp)){
        sp.foreach(x => println(Explorer.Types.nameToString(x)))
      }
    })
    //1-2
    //3-4
  }
}