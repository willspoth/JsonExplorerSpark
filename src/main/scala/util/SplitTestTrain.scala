package util

import org.apache.spark.rdd.RDD
import CMDLineParser.createSparkSession
import org.apache.spark.sql.SparkSession

object SplitTestTrain {

  def split(spark:SparkSession, fileName: String, trainPercent: Double, outputTrain: Boolean, validationSize: Int, outputValidation: Boolean): Unit = {
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
      .randomSplit(Array[Double](trainSize,validationSize.toDouble,overflow)) // read file
    val train: RDD[String] = data.head
    val validation: RDD[String] = data(1)

    val trainFileName: String = fileName + ".train" + "-" + trainPercent.toInt.toString + "-" + validationSize.toString
    val validationFileName: String = fileName + ".val" + "-" + trainPercent.toInt.toString + "-" + validationSize.toString

    if (outputTrain) train.saveAsTextFile(trainFileName)
    if (outputValidation) validation.saveAsTextFile(validationFileName)

    println(s"""Done splitting file: ${fileName} into ${trainFileName} of size ${train.count()} and ${validationFileName} of size ${validation.count()}""")
  }

  def main(args: Array[String]): Unit = {

    val fileName: String = args(0)
    val outputTrain: Boolean = args(2).equals("true")
    val validationSize: Int = args(3).toInt
    val outputValidation: Boolean = args(4).equals("true")
    val config: Option[String] = if(args.size == 6) Some(args(5)) else None

    val spark = createSparkSession(config)
    // 3,321,936

//    println(
//      spark.sparkContext.textFile(fileName)
//        .filter(_.size > 0)
//        .filter(_.head == '{')
//        .count()
//    )
//    ???
    List(10.0,20.0,30.0,40.0,50.0,70.0,90.0).foreach(trainPercent => split(spark,fileName,trainPercent,outputTrain,validationSize,outputValidation))

    //Clean.githubAll("/home/will/Data/jsonData/githubSigmod2020/github/","/home/will/Data/jsonData/githubSigmod2020.json")

  }

}
