import Explorer._
import Extractor.Extract
import Seralize.Serializer
import org.apache.spark.{SparkConf, SparkContext}


object SparkMain {

  //-Xmx6g

  def main(args: Array[String]) = {


    val startTime = System.currentTimeMillis() // Start timer


    val conf = new SparkConf()//.set("spark.driver.maxResultSize", "4g").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
      .setMaster("local[*]").setAppName("JSON Typing")
    val spark = new SparkContext(conf)

    val inputLocation = args(0)
    val KSE_Threshold: Double = 2.0


    val records = spark.textFile(inputLocation)
    val root = records
      .filter(x => (x.size > 0 && x.charAt(0).equals('{'))) // filter out lines that aren't Json
      .mapPartitions(x => Serializer.serialize(x)) // serialize output
      .mapPartitions(x => Extract.ExtractAttributes(x)).reduce(Extract.combineAllRoots(_,_)) // extraction phase

    root.AllAttributes.foreach{case(name,attribute) => {
      attribute.keySpaceEntropy = Some(keySpaceEntropy(attribute.types))
      // now need to make operator tree
      name.foldLeft(root.GrandTree){case(tree,n) => {

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
            tree.put(n,Some(new node()))
            tree.get(n).get.get
        }
      }}

    }}


    // traverse tree depth first and retype
    // nodes that don't pass
    def rewriteRoot(tree: node, name: scala.collection.mutable.ListBuffer[Any]): Unit = {
      tree.foreach{case(local_name,local_node) => {
        local_node match {
          case Some(temp) =>
            if(!temp.isEmpty)
            // first call deeper, then check
            rewriteRoot(temp, name :+ local_name)
          case None => // check type, don't need to because leafs can't be obj arrays or var objects
        }
      }}
      // now call children have been rewritten if needed, so can check this and return
      if(name.isEmpty)
        return
      val attribute = root.AllAttributes.get(name).get
      // check if it's a special type
      val arrayOfObjects: Boolean = attribute.types.foldLeft(true){case(arrayofObjects,(types,count)) => {
        types.getType() match {
          case JE_Array(xs) => isArrayOfObjects(xs) && arrayofObjects
          case JE_Obj_Array(xs) => isArrayOfObjects(xs) && arrayofObjects
          case JE_Empty_Array | JE_Null => arrayofObjects
          case _ => false
        } // end match
      }}
      // check if it's an array of objects
      if(arrayOfObjects) {
        attribute.naiveType = JE_Obj_Array
      } else if(attribute.keySpaceEntropy.get > KSE_Threshold) {
        if(attribute.types.foldLeft(true){case(varObject,(types,count)) => {
          types.getType() match {
            case JE_Object | JE_Null | JE_Empty_Object => varObject
            case _ => false
          }
        }})
        attribute.naiveType = JE_Var_Object
      }

    }

    rewriteRoot(root.GrandTree,scala.collection.mutable.ListBuffer[Any]())


    // bottom up, if varObj or arrayOfObj then separate into new schema unless parent is an array

    def getChildren(tree: node, collector: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute], name: scala.collection.mutable.ListBuffer[Any]): Unit = {
      tree.foreach{case(local_name,local_node) => {
        local_node match {
          case Some(temp) =>
            if(!temp.isEmpty)
              getChildren(temp, collector, name :+ local_name)
        }
      }}
      collector.put(name,root.AllAttributes.get(name).get)
    }

    def pullOutNode(tree: node, name: scala.collection.mutable.ListBuffer[Any]): Unit = {
      val jes = new JsonExtractionSchema()
      jes.tree = tree
      jes.parent = name
      getChildren(tree, jes.attributes, name)
      root.schemas += jes

      jes.attributes.foreach{case(n,a) => root.localAttributes.remove(n)}
      root.computationTree = buildNodeTree(root.localAttributes)
    }


    /*

     */

    def separateSchemas(tree: node, name: scala.collection.mutable.ListBuffer[Any]): Boolean = {
      tree.foreach{case(local_name,local_node) => {
        local_node match {
          case Some(temp) =>
            if(!temp.isEmpty) {
              // first call deeper, then check
              if(separateSchemas(temp, name :+ local_name)){
                if(!name.isEmpty){
                  root.AllAttributes.get(name).get.naiveType match {
                    case JE_Var_Object | JE_Obj_Array => // do nothing because parent will handle it
                    case _ => pullOutNode(tree.get(local_name).get.get,name :+ local_name)
                  }
                } else {
                  pullOutNode(tree.get(local_name).get.get,name :+ local_name)
                }
              }
            }
          case None => // check type, don't need to because leafs can't be obj arrays or var objects
        }
      }}

      if(name.isEmpty)
        return false
      val attribute = root.AllAttributes.get(name).get
      attribute.naiveType match {
        case JE_Var_Object | JE_Obj_Array => return true
        case _ => return false
      }

    }

    root.computationTree = buildNodeTree(root.AllAttributes)
    root.localAttributes = root.AllAttributes
    separateSchemas(root.computationTree,scala.collection.mutable.ListBuffer[Any]())

    // this is the main schema, adding it as a convenience, this way root.schemas captures all the required information
    val mainSchema = new JsonExtractionSchema()
    root.localAttributes.foreach{case(n,a) => mainSchema.attributes.put(n,a)}
    mainSchema.parent = new scala.collection.mutable.ListBuffer[Any]
    mainSchema.tree = buildNodeTree(mainSchema.attributes)
    root.schemas.prepend(mainSchema)

/*
    root.schemas.foreach(schema => {
      schema.attributes.foreach{case(name,attr) => {
        name.last match {
          case i:Int => // this is an array with no children that are objects so truncate the array
            schema.attributes.remove(name)
          case _ =>
        }
      }}
    })
*/
    root.schemas.foreach(schema => {
      schema.attributes.foreach{case(name,attr) => {
        val newName = name.map(n => {
          n match {
            case i:Int => Star
            case _ => n
          }
        })
        schema.attributes.remove(name)
        schema.attributes.get(newName) match {
          case Some(oldAttr) =>
            val newAttribute = new Attribute
            newAttribute.naiveType = oldAttr.naiveType // should change
            newAttribute.name = newName
            newAttribute.types = oldAttr.types ++ attr.types
            newAttribute.keySpaceEntropy = oldAttr.keySpaceEntropy // should change
            schema.attributes.put(newName,new Attribute)
          case None =>
            schema.attributes.put(newName,attr)
        }
      }}
    })
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


  def isArrayOfObjects(xs:List[JsonExplorerType]): Boolean = {
    (xs.foldLeft(true){case(bool,x) => { // with only objects for children
      x.getType() match {
        case JE_Object| JE_Var_Object | JE_Obj_Array | JE_Null | JE_Empty_Object | JE_Empty_Array => bool
        case _ => false
      }
    }} && xs.size > 0)
  }

  def buildNodeTree(attributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute]): node = {
    val tree = new node()
    attributes.foreach{case(name,attribute) => {
      // now need to make operator tree
      name.foldLeft(tree){case(tree,n) => {

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
            tree.put(n,Some(new node()))
            tree.get(n).get.get
        }
      }}

    }}
    return tree
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
