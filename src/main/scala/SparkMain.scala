import Explorer._
import Extractor.{Extract, ExtractorPhase}
import Seralize.{LastSeralizer, Serializer, SimpleSeralizer}
import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Json


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
      .setMaster("local[*]").setAppName("JSON Typing")
    val spark = new SparkContext(conf)

    val inputLocation = args(0)


    val records = spark.textFile(inputLocation)
    val root = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{'))) // filter out lines that aren't Json
      .mapPartitions(x => Serializer.serialize(x)) // serialize output
      .mapPartitions(x => Extract.ExtractAttributes(x)).reduce(Extract.combineAllRoots(_,_)) // extraction phase

    root.attributes.foreach{case(name,attribute) => {
      attribute.keySpaceEntropy = Some(keySpaceEntropy(attribute.types))
      // now need to make operator tree
      name.foldLeft(root.tree){case(tree,n) => {
        tree.get(n) match {
          case Some(opNode) =>
            opNode match {
              case Some(nod) =>
                tree.get(n).get.get
              case None =>
                tree.put(n,Some(new node()))
                tree.get(n).get.get
            }
          case None =>
            tree.put(n,None)
            tree
        }
      }}

    }}

    // traverse tree depth first and retype
    // nodes that don't pass
    def rewriteRoot(tree: node, name: scala.collection.mutable.ListBuffer[Any]): Unit = {
      tree.foreach{case(local_name,local_node) => {
        local_node match {
          case Some(temp) =>
            // first call deeper, then check
            rewriteRoot(temp, name :+ local_name)
          case None => // check type, don't need to because leafs can't be obj arrays or var objects
        }
      }}
      // now call children have been rewritten if needed, so can check this and return
      val attribute = root.attributes.get(name).get
      // check if it's a special type
      attribute.types.foldLeft((true,true)){case((arrayofObjects,variableObject),(types,count)) => {

        (arrayofObjects,variableObject)
      }}
    }

    rewriteRoot(root.tree,scala.collection.mutable.ListBuffer[Any]())
    // create list of split schemas
    // create feature vectors from this list

    // run nmf and display results


    val endTime = System.currentTimeMillis() // End Timer
    println("Total execution time: " + (endTime - startTime) + " ms") // Output run time in milliseconds

  }

  def keySpaceEntropy(m:scala.collection.mutable.Map[JsonExplorerType,Int]): Double = {
    val total: Int = m.foldLeft(0){(count,x) => {
      x._1 match {
        case JE_Empty_Array | JE_Empty_Object => count
        case _ => count + x._2
      }
    }}
    val entropy: Double = m.foldLeft(0.0){(ent,x) => {
      x._1 match {
        case JE_Empty_Array | JE_Empty_Object => ent
        case _ =>
          val p: Double = x._2.toDouble / total
          ent + (p * scala.math.log(p))
      }
    }}
    if(entropy == 0.0)
      return entropy
    else
      return -1.0 * entropy
  }

/*
  def typeEntropy(typeObject: JsonExplorerType): Double = {




    val m: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map[String,Int]()
    unpackedTypeList.split(",").map(_.split(":")(1)).foreach(x => {
      m.get(x) match {
        case Some(c) => m.update(x,c+1)
        case None => m.update(x,1)
      }
    })
    val total: Int = m.foldLeft(0){(count,x) => count + x._2}
    val entropy: Double = m.foldLeft(0.0){(ent,x) => {
      val p: Double = x._2.toDouble/total
      ent + (p*scala.math.log(p))
    }}
    if(entropy == 0.0)
      return entropy
    else
      return -1.0 * entropy
  }
*/

}
