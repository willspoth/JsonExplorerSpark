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
                    spark: SparkSession,
                    memory: Option[Boolean],
                    k: Int,
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


    val k: Int = argMap.get("k") match {
      case Some(s) =>
        try {
          s.toInt
        } catch {
          case e: Exception => throw new Exception("Make sure val is an integer in the form -k 7")
        }
      case _ | None => 0
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

    val testMode: Boolean = argMap.get("testMode") match {
      case Some("true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false") => false
      case _ | None => false
    }

    val writeJsonSchema: Boolean = argMap.get("schema") match {
      case Some("true" | "t" | "y" | "yes") => true
      case Some("n" | "no" | "false" | "f") => false
      case _ | None => true
    }

    val logFileName: String = argMap.get("log") match {
      case Some(s) => s
      case _ | None => filename.split("/").last.split("-").head+".USlog"
    }

    // spark config
    val spark = createSparkSession(argMap.get("config"))

    return config(filename,logFileName, spark.sparkContext.textFile(filename), spark, memory, k, kse,spark.conf.get("name").toString, writeJsonSchema, argMap)
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

}
