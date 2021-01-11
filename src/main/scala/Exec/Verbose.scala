package Exec

import java.io.FileWriter
import java.util.Calendar

import Extractor.Types.AttributeName
import Extractor.{Attribute, Extract, JE_Boolean, JE_Numeric, JE_String, JacksonShredder, JsonExplorerType}
import util.Log.LogOutput
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Verbose {

  private def shredRecords(input: RDD[String]): RDD[JsonExplorerType] =
    input.mapPartitions(x=>JacksonShredder.shred(x))

  private def extractTypeStructure(shreddedRecords: RDD[JsonExplorerType]): Set[JsonExplorerType] =
    shreddedRecords.distinct().collect().toSet

  private def validateRows(schemas: Set[JsonExplorerType], validationSet: RDD[String]): Double = {
    val validationSetSize: Double = validationSet.count().toDouble
    if(validationSetSize > 0)
      return shredRecords(validationSet).map(x => if(schemas.contains(x)) 1.0 else 0.0).reduce(_+_) / validationSetSize
    else return 0.0
  }


  def run(train: RDD[String], validate: RDD[String], log: mutable.ListBuffer[LogOutput]): Unit = {
    val schemas = extractTypeStructure(shredRecords(train)) // issue with multiple different types and nulls
    log += LogOutput("Precision", schemas.size.toString, "Precision: ")
    log += LogOutput("Recall", validateRows(schemas, validate).toString(), "Recall: ")
    log += LogOutput("Grouping", schemas.size.toString(), "Grouping: ")
  }

  def main(args: Array[String]): Unit = {
    val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()
    log += LogOutput("Date",Calendar.getInstance().getTime().toString,"Date: ")

    val config = util.CMDLineParser.readArgs(args) // Creates the Spark session with its config values.

    log += LogOutput("inputFile",config.fileName,"Input File: ")

    val startTime = System.currentTimeMillis() // Start timer

    Exec.Verbose.run(config.train,config.validation, log)

    val endTime = System.currentTimeMillis() // End Timer

    log += LogOutput("TotalTime",(endTime - startTime).toString,"Total execution time: ", " ms")
    log += LogOutput("TrainPercent",config.trainPercent.toString,"TrainPercent: ")
    log += LogOutput("ValidationSize",config.validationSize.toString,"ValidationSize: ")
    log += LogOutput("Algorithm","verbose","Algorithm: ")
    log += LogOutput("Seed",config.seed match {
      case Some(i) => i.toString
      case None => "None"},"Seed: ")

    config.spark.conf.getAll.foreach{case(k,v) => log += LogOutput(k,v,k+": ")}
    log += LogOutput("kse",config.kse.toString,"KSE: ")



    val logFile = new FileWriter(config.logFileName,true)
    logFile.write("{" + log.map(_.toJson).mkString(",") + "}\n")
    logFile.close()
    println(log.map(_.toString).mkString("\n"))
  }


}
