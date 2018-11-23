import Extractor.ExtractorPhase
import Seralize.{Seralizer, SimpleSeralizer}
import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}

object SparkMain {

  // this is a map of file names to file locations and options
  val dataList = Map[String,(String,Boolean)](("twitter"->("C:\\Users\\Will\\Documents\\GitHub\\JsonExplorer\\data\\dump-30.txt",true)),("yelp"->("C:\\Users\\Will\\Documents\\GitHub\\JsonExplorer\\data\\yelp_dataset",true)),("nasa"->("test/data/nasa.json",false)),
    ("phonelab"->("test/data/carlDataSample.out",false)),("enron"->("test/data/enron.json",true)),("medicine"->("C:\\Users\\Will\\Documents\\GitHub\\JsonExplorer\\data\\medicine.json",true)),("meteorite"->("test/data/meteorite.json",false)),("test"->("test/data/testTypes.json",true))
    ,("citi"->("cleanJsonOutput/citiStations.json",true)),("weatherUG"->("cleanJsonOutput/WUG.json",true)),("github"->("jsonDatasets/github.json",true)))

  val Gson = new Gson()

  def main(args: Array[String]) = {


    val startTime = System.currentTimeMillis() // Start timer

    val dataset: String = "yelp"

    val conf = new SparkConf().set("spark.driver.maxResultSize", "4g").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
      .setMaster("local[*]").setAppName("JSON Typing")
    val spark = new SparkContext(conf)


    val records = spark.textFile(dataList.get(dataset).get._1)
    val typeMap = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{'))) // filter out lines that aren't Json
      //.mapPartitions(x=> Seralizer.seralize(x)).collect()
        .mapPartitions(x=>Seralizer.seralize(x)).m


      //.mapPartitions(x=> ExtractorPhase.mapTypes(x))
      //.groupByKey(ExtractorPhase.reduceTypes(_,_)).collect()
      //.reduceByKey(ExtractorPhase.reduceTypes(_,_)).collect()

    println(typeMap)
    val endTime = System.currentTimeMillis() // End Timer
    println("Total execution time: " + (endTime - startTime) + " ms") // Output run time in milliseconds

  }
}
