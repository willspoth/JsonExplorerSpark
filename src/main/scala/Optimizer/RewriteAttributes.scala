package Optimizer

import Explorer.{Attribute, AttributeTree, JE_Array, JE_Empty_Array, JE_Empty_Object, JE_Null, JE_Obj_Array, JE_Object, JE_Tuple, JE_Var_Object, JsonExplorerType, Types}
import Explorer.Types.AttributeName

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object RewriteAttributes {

  private def upgradeType(base: Either[scala.collection.mutable.Set[JsonExplorerType],JsonExplorerType],
                          find: JsonExplorerType,
                          replace: JsonExplorerType
                         ): Either[scala.collection.mutable.Set[JsonExplorerType],JsonExplorerType] =
  {
    base match {
      case Right(jet) =>
        if(jet.getType().equals(find.getType()))
          return Right(replace.getType())
        else throw new Exception("You probably shouldn't end up here, check this out future me")
      case Left(jetSet) =>
        if(jetSet.contains(find.getType())) {
          jetSet.remove(find.getType())
          jetSet.add(replace.getType())
          return Left(jetSet)
        }
        else throw new Exception("You probably shouldn't end up here, check this out future me")
    }
  }

  private def updateAttributeType(attribute: Attribute,
                                  newType: Either[scala.collection.mutable.Set[JsonExplorerType],JsonExplorerType]
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

  def unwrap(t: Either[mutable.Set[JsonExplorerType],JsonExplorerType]): mutable.Set[JsonExplorerType] = {
    t match {
      case Right(b) => mutable.Set(b)
      case Left(s) => s
    }
  }

  private def getChildrenTypes(tree: AttributeTree
                              ): mutable.Set[JsonExplorerType] =
  {
    unwrap(tree.attribute.`type`) ++ tree.children.flatMap(x => getChildrenTypes(x._2))
  }

  private def checkObjectKSE(attributeTree: AttributeTree,
                             keySpaceThreshold: Double
                            ): Attribute =
  {
    if(!attributeTree.attribute.objectMarginalKeySpaceEntropy.equals(None)) {
      if ((attributeTree.attribute.objectMarginalKeySpaceEntropy.get > keySpaceThreshold) != (attributeTree.attribute.objectJointKeySpaceEntropy.get > keySpaceThreshold))
        println("KSE difference between marginal and joint exists and effects choice for attribute " + Types.nameToString(attributeTree.attribute.name) + " marginal: " + attributeTree.attribute.objectMarginalKeySpaceEntropy.get.toString + " joint: " + attributeTree.attribute.objectJointKeySpaceEntropy.get.toString)
      if ((attributeTree.attribute.objectMarginalKeySpaceEntropy.get >= keySpaceThreshold) && (getChildrenTypes(attributeTree).filterNot(_.equals(JE_Null)).filter(_.isBasic()).size <= 1)) {
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
            atLeastOneObjectInArray = !xs.filter(_.getType().equals(JE_Object)).filter(_.getType().equals(JE_Empty_Object)).isEmpty
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

  def attributeTreeToAttributeList(attributeTree: AttributeTree): mutable.HashMap[AttributeName,Attribute] = {
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


  def getSchemas(attributeTree: AttributeTree): mutable.Set[AttributeName] = {
    val schemas: mutable.Set[AttributeName] = mutable.Set[AttributeName]()
    getSchemas(attributeTree,schemas)
    return schemas
  }

  // returns a set of names that are variable_objects and arrays_of_objects
  private def getSchemas(attributeTree: AttributeTree, schemas: mutable.Set[AttributeName]): Unit = {
    if(attributeTree.name.equals("$")) // is root
      schemas.add(ListBuffer[Any]())
    else if(unwrap(attributeTree.attribute.`type`).map(_.getType()).contains(JE_Var_Object))
      schemas.add(attributeTree.attribute.name)
    else if(unwrap(attributeTree.attribute.`type`).map(_.getType()).contains(JE_Obj_Array))
      schemas.add(attributeTree.attribute.name)
    attributeTree.children.foreach(x => getSchemas(x._2, schemas))
  }

}
