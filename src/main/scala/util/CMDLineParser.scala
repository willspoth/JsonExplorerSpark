package util

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

object CMDLineParser {

  case class config(fileName: String,
                    logFileName: String,
                    train: RDD[String],
                    trainPercent: Double,
                    validationSize: Int,
                    seed: Option[Int],
                    spark: SparkSession,
                    memory: Option[Boolean],
                    runBiMax: Boolean,
                    kse: Double,
                    name: String,
                    writeJsonSchema: Boolean,
                    argMap: mutable.HashMap[String, String],
                    generateDot: Boolean = false,
                    useUI: Boolean = false
                   )

  def readArgs(args: Array[String]): config = {
    if(args.size == 0 || args.size%2 == 0) {
      println("Unexpected Argument, should be, filename -master xxx -name xxx -sparkinfo xxx -sparkinfo xxx")
      System.exit(0)
    }
    val argMap = scala.collection.mutable.HashMap[String,String]()
    val filename: String = args(0)
    if(args.tail.size > 1) {
      val argPairs = args.tail.zip(args.tail.tail).zipWithIndex.filter(_._2%2==0).map(_._1).foreach(x=>argMap.put(x._1,x._2))
    }

    val memory: Option[Boolean] = argMap.get("memory") match {
      case Some("memory" | "inmemory" | "true" | "t" | "y" | "yes") => Some(true)
      case Some("n" | "no" | "false" | "disk") => Some(false)
      case _ | None => None
    }

    val kse: Double = argMap.get("kse") match {
      case Some(s) =>
        try {
          s.toDouble
        } catch {
          case e: Exception => throw new Exception("Make sure kse is a double in the form kse 1.0, make 0.0 to disable")
        }
      case _ | None => 0.0
    }

    val writeJsonSchema: Boolean = argMap.get("schema") match {
      case Some("true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false" | "f") => false
      case _ | None => true
    }

    val runBiMax: Boolean = argMap.get("bimax") match {
      case Some("true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false" | "f") => false
      case _ | None => true
    }

    val logFileName: String = argMap.get("log") match {
      case Some(s) => s
      case _ | None => (new java.io.File(filename)).getName.split("-").head+".USlog"
    }

    val seed: Option[Int] = argMap.get("seed") match {
      case Some(s) => Some(s.toInt)
      case None => None
    }

    val trainPercent: Double = argMap.get("train") match {
      case Some(v) => v.toDouble
      case None => 100.0
    }

    val validationSize: Int = argMap.get("val") match {
      case Some(v) => v.toInt
      case None => 0
    }

    val numberOfRows: Option[Int] = argMap.get("numberOfRows") match {
      case Some(v) => Some(v.toInt)
      case None => None
    }

    // spark config
    val spark = createSparkSession(argMap.get("config"))

    val (train, validation) = split(spark, filename, trainPercent, validationSize, seed, numberOfRows)

    return config(filename, logFileName, train, trainPercent, validationSize, seed, spark, memory, runBiMax, kse,spark.conf.get("name").toString, writeJsonSchema, argMap)
  }


  // takes command line file location as override
  def createSparkSession(confFile: Option[String]): SparkSession = {

    val spark_conf_file: String = confFile.getOrElse(scala.util.Properties.envOrElse("SPARK_CONF_FILE",getClass.getResource("/spark.conf").getFile))
    val lines = Source.fromFile(spark_conf_file).getLines.toList

    val conf = new SparkConf()
    val args = lines.map(_.split("#").head).filter(s => !s.contains('#') && s.length > 0).map(s => {
      (s.split("=").head.trim,s.split("=").last.trim)
    })
    conf.setAll(args)
    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.config(conf).getOrCreate()
    return spark
  }

  def split(spark:SparkSession, fileName: String, trainPercent: Double, validationSize: Int, seed: Option[Int], totalRows: Option[Int]): (RDD[String],RDD[String]) = {
    val totalNumberOfLines: Long = totalRows match {
      case Some(i) => i
      case None =>
        spark.sparkContext.textFile(fileName).filter(x => (x.size > 0 && x.charAt(0).equals('{'))).count()
    }
    var trainSize: Double = totalNumberOfLines.toDouble*(trainPercent/100.0)
    if(trainPercent > 100.0)
      throw new Exception("Test Percent can't be higher than 100%, Found: " + trainPercent.toString)
    else if((trainSize + validationSize) > totalNumberOfLines) {
      trainSize = totalNumberOfLines.toDouble - validationSize.toDouble
      println("Total Percent can't be higher than 100%, Found: " + trainPercent.toString + " + " + (validationSize.toDouble/totalNumberOfLines.toDouble).toString + " setting test Percent to " + trainPercent.toString)
    }
    val overflow: Double = totalNumberOfLines.toDouble - validationSize.toDouble - trainSize.toDouble
    val data: Array[RDD[String]] = if(seed.equals(None)) {
      spark.sparkContext.textFile(fileName).filter(x => (x.size > 0 && x.charAt(0).equals('{')))
        .randomSplit(Array[Double](trainSize, validationSize.toDouble, overflow))
    } else {
      spark.sparkContext.textFile(fileName).filter(x => (x.size > 0 && x.charAt(0).equals('{')))
        .randomSplit(Array[Double](trainSize, validationSize.toDouble, overflow), seed = seed.get)
    } // read file
    val train: RDD[String] = data.head
    val validation: RDD[String] = data(1)

    return (train, validation)
  }

}
