package Optimizer

import Extractor.{Attribute, AttributeTree, GenericTree, JE_Array, JE_Empty_Array, JE_Empty_Object, JE_Null, JE_Obj_Array, JE_Object, JE_Tuple, JE_Var_Object, JsonExplorerType, Star, Types}
import Extractor.Types.{AttributeName, BiMaxNode}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object RewriteAttributes {

  private def upgradeType(base: scala.collection.mutable.Set[JsonExplorerType],
                          find: JsonExplorerType,
                          replace: JsonExplorerType
                         ): scala.collection.mutable.Set[JsonExplorerType] =
  {
    base.remove(find.getType())
    base.add(replace.getType())
    return base
  }

  private def updateAttributeType(attribute: Attribute,
                                  newType: scala.collection.mutable.Set[JsonExplorerType]
                                 ): Attribute =
  {
    new Attribute(
      attribute.name,
      newType,
      attribute.typeList,
      attribute.objectTypeEntropy,
      attribute.objectMarginalKeySpaceEntropy,
      attribute.objectJointKeySpaceEntropy,
      attribute.arrayTypeEntropy,
      attribute.arrayKeySpaceEntropy,
      attribute.properties,
      attribute.items
    )
  }

  private def getChildrenTypes(tree: AttributeTree
                              ): mutable.Set[JsonExplorerType] =
  {
    tree.attribute.`type` ++ tree.children.flatMap(x => getChildrenTypes(x._2))
  }

  private def checkObjectKSE(attributeTree: AttributeTree,
                             keySpaceThreshold: Double
                            ): Attribute =
  {
    if(!attributeTree.attribute.objectMarginalKeySpaceEntropy.equals(None)) {
      if (((attributeTree.attribute.objectMarginalKeySpaceEntropy.get >= keySpaceThreshold) && (getChildrenTypes(attributeTree).filterNot(_.equals(JE_Null)).filter(_.isBasic()).size <= 1)) && keySpaceThreshold > 0.0) {
        return updateAttributeType(attributeTree.attribute, upgradeType(attributeTree.attribute.`type`, JE_Object, JE_Var_Object))
      }
    }
    attributeTree.attribute
  }

  private def checkArrayKSE(attribute: Attribute,
                             keySpaceThreshold: Double
                            ): Attribute =
  {
    attribute.arrayKeySpaceEntropy match {
      case Some(kse) =>
        if(kse <= keySpaceThreshold)
          updateAttributeType(attribute, upgradeType(attribute.`type`, JE_Array, JE_Tuple))
        else
          attribute
      case None => attribute
    }
  }

  private def convertArrayOfObjects(attribute:Attribute, attributeMap: scala.collection.mutable.HashMap[JsonExplorerType,Int]
                                   ): Attribute =
  {
    var atLeastOneObjectInArray: Boolean = false
    val arraysWithObjects = attributeMap.map{ case(attr,count) => {
      attr match {
        case JE_Array(xs) =>
          if(!atLeastOneObjectInArray)
            atLeastOneObjectInArray = !xs.filter(x => x.getType().equals(JE_Object) || x.getType().equals(JE_Empty_Object)).isEmpty
          xs.filterNot(_.getType().equals(JE_Object)).filterNot(_.getType().equals(JE_Empty_Object)).isEmpty // array with only objects
        case JE_Empty_Array =>
          true
        case _ =>
          false
      }
    }}

    if (arraysWithObjects.isEmpty)
      return attribute
    else if (arraysWithObjects.reduce(_&&_) && atLeastOneObjectInArray)
      return updateAttributeType(attribute, upgradeType(attribute.`type`, JE_Array, JE_Obj_Array))
    else
      return attribute
  }


  def checkForTupleArrayOfObjects(attributeTree: AttributeTree): Unit = {
    if(!attributeTree.name.equals("$") && attributeTree.attribute.`type`.contains(JE_Array)) { // skip root
      val allObjs: Boolean = attributeTree.attribute.typeList.forall(x => {
        x._1 match {
          case JE_Array(a) => a.forall(y => y.equals(JE_Object) || y.equals(JE_Empty_Object) )
          case _ => false
        }
      })

      val allSameSize: Boolean = attributeTree.attribute.typeList.flatMap(x => {
        x._1 match {
          case JE_Array(a) => List(a.size)
          case JE_Empty_Array => List(0)
          case _ => List()
        }
      }).filter(_ > 1).toSet.size == 1

      //if(allObjs && allSameSize)
        //throw new Exception("Static Object array Found: " + Types.nameToString(ListBuffer(attributeTree.name)))
    }

    attributeTree.children.foreach(x => checkForTupleArrayOfObjects(x._2))
  }


  def rewriteSemanticTypes(attributeTree: AttributeTree,
                           objectKeySpaceThreshold: Double,
                           arrayKeySpaceThreshold: Double,
                           typeThreshold: Double
                          ): Unit =
  {
    // depth first for variable array constraint on same type
    attributeTree.children.foreach(x => rewriteSemanticTypes(x._2,objectKeySpaceThreshold,arrayKeySpaceThreshold,typeThreshold))

    if(!attributeTree.name.equals("$")) { // skip root
      attributeTree.attribute = checkObjectKSE(attributeTree, objectKeySpaceThreshold)
      //attributeTree.attribute = checkArrayKSE(attributeTree.attribute,arrayKeySpaceThreshold)
      attributeTree.attribute = convertArrayOfObjects(attributeTree.attribute, attributeTree.attribute.typeList)
    }

  }

  def GenericListToGenericTree[T](list: Array[(AttributeName,T)]): GenericTree[T] = {
    val gTree: GenericTree[T] = new GenericTree[T]("$",mutable.HashMap[Any,GenericTree[T]](), null.asInstanceOf[T])

    list.foreach{case(name, v) => {
      if(name.isEmpty) // root
        gTree.payload = v
      else {
        val last = name.foldLeft(gTree: GenericTree[T]){ case(at, n) => {
          at.children.get(n) match {
            case Some(at2) => at2
            case None =>
              val at2 = new GenericTree[T](n,mutable.HashMap[Any,GenericTree[T]](),null.asInstanceOf[T])
              at.children.put(n,at2)
              at2
          }
        }}
        last.payload = v
        last
      }
    }}

    gTree
  }


  def attributeListToAttributeTree(attributeList: Array[(AttributeName,Attribute)]
                                  ): AttributeTree =
  {
    val attributeTree: AttributeTree = new AttributeTree("$",mutable.HashMap[Any,AttributeTree](),null)

    attributeList.foreach(a => {
      if(a._1.isEmpty) // root
        attributeTree.attribute = a._2
      else {
        val last = a._1.foldLeft(attributeTree: AttributeTree){ case(at, n) => {
          at.children.get(n) match {
            case Some(at2) => at2
            case None =>
              val at2 = new AttributeTree(n,mutable.HashMap[Any,AttributeTree](),null)
              at.children.put(n,at2)
              at2
          }
        }}
        last.attribute = a._2
        last
      }
    })

    attributeTree
  }


  def attributeTreeToAttributeMap(attributeTree: AttributeTree): mutable.HashMap[AttributeName,Attribute] = {
    val attributeMap = mutable.HashMap[AttributeName,Attribute]()
    def explode(attributeTree: AttributeTree): Unit = {
      if(attributeTree.attribute != null) {
        attributeMap.put(attributeTree.attribute.name, attributeTree.attribute)
      }

      attributeTree.children.foreach(x => explode(x._2))
    }
    explode(attributeTree)
    attributeMap
  }



  def getSchemas(attributeTree: AttributeTree): mutable.Set[(AttributeName,JsonExplorerType)] = {
    val schemas = mutable.Set[(AttributeName,JsonExplorerType)]()
    getSchemas(attributeTree,schemas)
    return schemas
  }

  // returns a set of names that are variable_objects and arrays_of_objects
  private def getSchemas(attributeTree: AttributeTree, schemas: mutable.Set[(AttributeName,JsonExplorerType)]): Unit = {
    if(attributeTree.name.equals("$")) // is root
      schemas.add(ListBuffer[Any](),JE_Object)
    else if(attributeTree.attribute.`type`.map(_.getType()).contains(JE_Var_Object))
      schemas.add((attributeTree.attribute.name,JE_Var_Object))
    else if(attributeTree.attribute.`type`.map(_.getType()).contains(JE_Obj_Array))
      schemas.add((attributeTree.attribute.name,JE_Obj_Array))
    attributeTree.children.foreach(x => getSchemas(x._2, schemas))
  }

  def pickFromTree[T](attributeName: AttributeName, tree: GenericTree[T], currentName: AttributeName = ListBuffer[Any]()): GenericTree[T] = {
    if(attributeName.isEmpty)
      return tree
    else { // need to navigate to right location because q.q
      if (currentName.equals(attributeName))
        return tree
      else
        return pickFromTree(attributeName,tree.children.get(attributeName(currentName.size)).get,currentName ++ List(attributeName(currentName.size)))
    }
  }

}
