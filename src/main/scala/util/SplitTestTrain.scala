package util

import org.apache.spark.rdd.RDD
import CMDLineParser.createSparkSession
import org.apache.spark.sql.SparkSession

object SplitTestTrain {

  def split(spark:SparkSession, fileName: String, trainPercent: Double, outputTrain: Boolean, validationSize: Int, outputValidation: Boolean, Seed: Int): Unit = {
    val totalNumberOfLines: Long = spark.sparkContext.textFile(fileName).filter(x => (x.size > 0 && x.charAt(0).equals('{'))).count()
    println("total lines: " + totalNumberOfLines.toString)
    var trainSize: Double = totalNumberOfLines.toDouble*(trainPercent/100.0)
    if(trainPercent > 100.0)
      throw new Exception("Test Percent can't be higher than 100%, Found: " + trainPercent.toString)
    else if((trainSize + validationSize) > totalNumberOfLines) {
      trainSize = totalNumberOfLines.toDouble - validationSize.toDouble
      println("Total Percent can't be higher than 100%, Found: " + trainPercent.toString + " + " + (validationSize.toDouble/totalNumberOfLines.toDouble).toString + " setting test Percent to " + trainPercent.toString)
    }
    val overflow: Double = totalNumberOfLines.toDouble - validationSize.toDouble - trainSize.toDouble
    val data: Array[RDD[String]] = spark.sparkContext.textFile(fileName).filter(x => (x.size > 0 && x.charAt(0).equals('{')))
      .randomSplit(Array[Double](trainSize,validationSize.toDouble,overflow),seed=Seed) // read file
    val train: RDD[String] = data.head
    val validation: RDD[String] = data(1)

    val trainFileName: String = fileName + ".train" + "-" + trainPercent.toInt.toString + "-" + validationSize.toString
    val validationFileName: String = fileName + ".val" + "-" + trainPercent.toInt.toString + "-" + validationSize.toString

    println(train.first())

    if (outputTrain) train.saveAsTextFile(trainFileName)
    if (outputValidation) validation.saveAsTextFile(validationFileName)

    //println(s"""Done splitting file: ${fileName} into ${trainFileName} of size ${train.count()} and ${validationFileName} of size ${validation.count()}""")
  }

  private def asRDD(spark: SparkSession, inputFileName:String, outputFileName: String): Unit = {
    val data: RDD[String] = spark.sparkContext.textFile(inputFileName).filter(x => (x.size > 0 && x.charAt(0).equals('{')))
    data.saveAsTextFile(outputFileName)
  }

  def main(args: Array[String]): Unit = {

    val inputFileName: String = args(0)
    val trainPercent: Double = args(1).toDouble
    val validationSize: Int = args(2).toInt
    val seed: Int = args(3).toInt
    val spark = if(args.size == 5) createSparkSession(Some(args(4))) else createSparkSession(None)

    split(spark,inputFileName,trainPercent,true,validationSize,true,seed)
  }

}
