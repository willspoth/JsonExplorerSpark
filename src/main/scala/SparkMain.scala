import Explorer.{JE_Array, JE_Numeric, JE_Object, JE_String}
import Extractor.{Extract, ExtractorPhase}
import Seralize.{LastSeralizer, Serializer, SimpleSeralizer}
import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Json

import scala.collection.mutable.ListBuffer

object SparkMain {

  // this is a map of file names to file locations and options
  val dataList = Map[String,(String,Boolean)](("twitter"->("C:\\Users\\Will\\Documents\\GitHub\\JsonExplorer\\data\\dump-30.txt",true)),("yelp"->("C:\\Users\\Will\\Documents\\GitHub\\JsonExplorer\\data\\yelp_dataset",true)),("nasa"->("test/data/nasa.json",false)),
    ("phonelab"->("test/data/carlDataSample.out",false)),("enron"->("test/data/enron.json",true)),("medicine"->("C:\\Users\\Will\\Documents\\GitHub\\JsonExplorer\\data\\medicine.json",true)),("meteorite"->("test/data/meteorite.json",false)),("test"->("test/data/testTypes.json",true))
    ,("citi"->("cleanJsonOutput/citiStations.json",true)),("weatherUG"->("cleanJsonOutput/WUG.json",true)),("github"->("jsonDatasets/github.json",true)))

  val Gson = new Gson()

  def main(args: Array[String]) = {

    //println(JE_Object(Map("i"->JE_String)).equals(JE_Object(Map("i"->JE_Numeric))))

    //println(JE_Array(List(JE_String,JE_Numeric)).equals(JE_Array(List(JE_String,JE_Numeric))))
    /*
    val m = scala.collection.mutable.HashMap[ListBuffer[Any],Int]()
    m.put(ListBuffer("A","B",0),1)
    m.put(ListBuffer("A","B",1),1)

    m.put(ListBuffer("A","B",0),m.get(ListBuffer("A","B",0)).get + 1)

    println(m)
    ???
    */
    val startTime = System.currentTimeMillis() // Start timer

    val dataset: String = "yelp"

    val conf = new SparkConf().set("spark.driver.maxResultSize", "4g").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
      .setMaster("local").setAppName("JSON Typing")
    val spark = new SparkContext(conf)

    val inputLocation = args(0)


    val records = spark.textFile(inputLocation)
    val typeMap = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{'))) // filter out lines that aren't Json
      .mapPartitions(x=> Serializer.serialize(x)) // serialize output
      .mapPartitions(x=> Extract.ExtractAttributes(x)).map(Extract.unwrapRoots(_)).reduce(Extract.combineAllRoots(_,_)) // extraction phase
      .collect()
      //  .map(x=>SimpleSeralizer.seralize(x)).reduce(Extract.createTypes(_,_))
        //.mapPartitions(x=>LastSeralizer.seralize(x)).collect()


      //.mapPartitions(x=> ExtractorPhase.mapTypes(x))
      //.groupByKey(ExtractorPhase.reduceTypes(_,_)).collect()
      //.reduceByKey(ExtractorPhase.reduceTypes(_,_)).collect()

    println(typeMap)
    val endTime = System.currentTimeMillis() // End Timer
    println("Total execution time: " + (endTime - startTime) + " ms") // Output run time in milliseconds

  }
}
