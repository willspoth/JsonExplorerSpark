package Optimizer

import Explorer.Types.AttributeName
import Explorer._

import scala.collection.mutable.ListBuffer


/** can call rewrite multiple times with different kse to change plan, call keep once happy with choice. Made to be compatible with Viz.PlannerFrame. The recovered JERoot schemas are what's used in the next phase.
  *
  * @param root JsonExtractionRoot
  */
class ConvertOperatorTree (root: JsonExtractionRoot) {

  var allAttributes: scala.collection.mutable.HashMap[AttributeName,Attribute] = null
  var localSchemas: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],JsonExtractionSchema] = null
  var computationTree: node = null
  var localAttributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute] = null


  def cloneTypes(attributes: scala.collection.mutable.HashMap[AttributeName,Attribute]): scala.collection.mutable.HashMap[AttributeName,Attribute] = {
    val a = attributes.map(x => Tuple2(x._1.clone(),x._2.clone()))
    a
  }


  def Rewrite(kse_threshold: Double): Unit = {
    allAttributes = cloneTypes(root.AllAttributes)
    localSchemas = scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],JsonExtractionSchema]()
    localAttributes = scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute]()
    rewriteRoot(root.Tree, scala.collection.mutable.ListBuffer[Any](), allAttributes, kse_threshold)

    computationTree = buildNodeTree(allAttributes)
    allAttributes.foreach{case(n,a) => localAttributes.put(n.clone(),a)}
    separateSchemas(computationTree,scala.collection.mutable.ListBuffer[Any]())

    // this is the main schema, adding it as a convenience, this way root.schemas captures all the required information
    val mainSchema = new JsonExtractionSchema()
    localAttributes.foreach{case(n,a) => mainSchema.attributes.put(n,a)}
    mainSchema.parent = new scala.collection.mutable.ListBuffer[Any]
    mainSchema.tree = buildNodeTree(mainSchema.attributes)
    mainSchema.naiveType = JE_Object
    localSchemas.put(mainSchema.parent,mainSchema)
  }

  // updates root
  def Keep(): Unit = {
    addStarToSchemas()
    localSchemas.foreach(s => {
      s._2.attributeLookup = scala.collection.mutable.HashMap[ListBuffer[Any],Int](s._2.attributes.map(_._1).zipWithIndex.toList: _*)
    })
    root.AllAttributes = allAttributes
    root.Schemas = localSchemas
  }


  // traverse tree depth first and retype
  // nodes that don't pass
  private def rewriteRoot(tree: node, name: scala.collection.mutable.ListBuffer[Any], allAttributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute], KSE_Threshold: Double): Unit = {
    tree.foreach{case(local_name,local_node) => {
      local_node match {
        case Some(temp) =>
          if(!temp.isEmpty)
          // first call deeper, then check
            rewriteRoot(temp, name :+ local_name, allAttributes, KSE_Threshold)
        case None => // check type, don't need to because leafs can't be obj arrays or var objects
      }
    }}
    // now call children have been rewritten if needed, so can check this and return
    if(name.isEmpty)
      return

    val attribute = allAttributes.get(name).get
    // check if it's a special type
    val arrayOfObjects: Boolean = attribute.types.foldLeft(true){case(arrayofObjects,(types,count)) => {
      types match {
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
      }}) {
        attribute.naiveType = JE_Var_Object
      }
    }

  }



  private def isArrayOfObjects(xs:ListBuffer[JsonExplorerType]): Boolean = {
    (xs.foldLeft(true){case(bool,x) => { // with only objects for children
      x.getType() match {
        case JE_Object| JE_Var_Object | JE_Obj_Array | JE_Null | JE_Empty_Object | JE_Empty_Array => bool
        case _ => false
      }
    }} && xs.size > 0)
  }


  // helper function to build a tree out of any set of attributes
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



  // bottom up, if varObj or arrayOfObj then separate into new schema unless parent is an array

  def getChildren(tree: node, collector: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute], name: scala.collection.mutable.ListBuffer[Any]): Unit = {
    tree.foreach{case(local_name,local_node) => {
      local_node match {
        case Some(temp) =>
          if(!temp.isEmpty)
            getChildren(temp, collector, name :+ local_name)
          else {
            allAttributes.get(name :+ local_name) match {
              case Some(a) => collector.put(name :+ local_name, a)
              case None => throw new Exception("Found attribute that is not known: "+(name :+ local_name).toString())
            }

          }

      }
    }}
    collector.put(name,allAttributes.get(name).get)
  }

  def pullOutNode(tree: node, name: scala.collection.mutable.ListBuffer[Any]): Unit = {
    val jes = new JsonExtractionSchema()
    jes.tree = tree
    jes.parent = name
    jes.naiveType = allAttributes.get(name).get.naiveType
    getChildren(tree, jes.attributes, name)
    localSchemas.put(jes.parent,jes)

    jes.attributes.remove(name)
    jes.attributes.foreach{case(n,a) => localAttributes.remove(n)}
    computationTree = buildNodeTree(localAttributes)
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
                allAttributes.get(name).get.naiveType match {
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
    val attribute = allAttributes.get(name).get
    attribute.naiveType match {
      case JE_Var_Object | JE_Obj_Array => return true
      case _ => return false
    }

  }

  /**
    * collapses arrays to single flat star schema
    */

  private def addStarToSchemas(): Unit = {
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
    // rewrite arrays to stars
    localSchemas.foreach{case(_,schema) => {
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
    }}
  }

}
