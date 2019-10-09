package util

import Explorer.{Attribute, AttributeTree, JE_Array, JE_Empty_Array, JE_Empty_Object, JE_Obj_Array, JE_Object, JE_Var_Object, JsonExplorerType, Types}
import Explorer.Types.{AttributeName, BiMaxNode, BiMaxStruct}
import Optimizer.RewriteAttributes
import util.JsonSchema.{JSA_additionalProperties, JSA_anyOf, JSA_description, JSA_items, JSA_maxItems, JSA_maxProperties, JSA_oneOf, JSA_properties, JSA_required, JSA_type, JSS, JsonSchemaStructure}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NodeToJsonSchema {


  private def biMaxNodeToAttribute(biMaxNode: BiMaxNode, attributeMap: mutable.HashMap[AttributeName,Attribute], attributeName: AttributeName): AttributeTree = {

    val updatedAttributeMap: Map[AttributeName,Attribute] =
      (mutable.ListBuffer((biMaxNode.types,biMaxNode.multiplicity)) ++ biMaxNode.subsets) // put together all schemas
      .flatMap(x => x._1.toList.map(y => (y._1,y._2,x._2))) // add multiplicity to each path and type, time to merge
      .groupBy(_._1)
      .map(x => {
        val v = x._2.reduce( (l,r) => (l._1,l._2++r._2,l._3+r._3 ) )
        (x._1,v._2,v._3)
      }).map(x => {
        (x._1,
          new Attribute(
            x._1,
            x._2,
            null, // TODO could go back and track this :/ not needed though
            attributeMap.getOrElse(x._1,new Attribute()).objectTypeEntropy,
            attributeMap.getOrElse(x._1,new Attribute()).objectMarginalKeySpaceEntropy,
            attributeMap.getOrElse(x._1,new Attribute()).objectJointKeySpaceEntropy,
            attributeMap.getOrElse(x._1,new Attribute()).arrayTypeEntropy,
            attributeMap.getOrElse(x._1,new Attribute()).arrayKeySpaceEntropy,
            null,
            null,
            false,
            x._3
          )
        )
      }).toMap

    RewriteAttributes.pickFromTree(attributeName,RewriteAttributes.attributeListToAttributeTree(updatedAttributeMap.toArray)) // need to unwrap because :/
  }

  private def attributeToJSS(
                              attributeTree: AttributeTree,
                              SpecialSchemas: Map[AttributeName,Types.DisjointNodes],
                              attributeMap: mutable.HashMap[AttributeName,Attribute],
                              parentMult: Int,
                              attributeName: AttributeName
                            ): JSS = {


    var attribute: Attribute = attributeTree.attribute
    if(attribute == null)
      attribute = attributeMap.get(attributeName).get // attempt to match up


    SpecialSchemas.get(attribute.name) match {
      case Some(djn) => makeBiMaxJsonSchema(djn,SpecialSchemas-attribute.name,attributeMap, attribute.name)
      case None =>
        // clean types
        val anyOf = attribute.`type`.filter( t => if (t.equals(JE_Empty_Object) && attribute.`type`.contains(JE_Object) || (t.equals(JE_Empty_Array) && attribute.`type`.contains(JE_Array)) ) false else true)
          .toList.map(t => {
          val seq: ListBuffer[JsonSchemaStructure] = ListBuffer(
            JSA_type(JsonExplorerTypeToJsonSchemaType.convert(t))
          )

          if(t.getType().equals(JE_Object) || t.getType().equals(JE_Var_Object)){
            seq.append(JSA_properties(
              attributeTree.children.map(x => (x._1.toString,attributeToJSS(
                x._2,
                SpecialSchemas,
                attributeMap,
                attribute.multiplicity,
                attribute.name ++ List(x._1)
              ))).toMap
            ))
          }

          if(t.getType().equals(JE_Array)){
            val items: Seq[JSS] = attributeTree.children.map(x => attributeToJSS(
              x._2,
              SpecialSchemas,
              attributeMap,
              attribute.multiplicity,
              attribute.name ++ List(x._1)
            )).toSeq

            val item: JSS = if (items.size > 1) JSS(Seq(JSA_oneOf(items))) else items.head
            seq.append(JSA_items(
              item
            ))
          }

          // embed entropy for debugging
          val description: String = {
            List(
              attribute.objectTypeEntropy match {
                case Some(v) => "ObjectTypeEntropy:" + v.toString
                case None => ""
              },
              attribute.objectMarginalKeySpaceEntropy match {
                case Some(v) => "ObjectMarginalKeySpaceEntropy:" + v.toString
                case None => ""
              },
              attribute.objectJointKeySpaceEntropy match {
                case Some(v) => "ObjectJointKeySpaceEntropy:" + v.toString
                case None => ""
              }
            ).filter(!_.equals("")).mkString(",")
          }

          if (description.size > 0) seq.append(JSA_description(description))

          // check for empty arrays
          if (t.getType().equals(JE_Empty_Object)) seq.append(JSA_maxProperties(0.0))
          if (t.getType().equals(JE_Empty_Array)) seq.append(JSA_maxItems(0.0))

          // check for variable objects
          if (t.getType().equals(JE_Var_Object)) seq.append(JSA_additionalProperties(true))

          // TODO array of objects
          if(t.getType().equals(JE_Obj_Array)){
            val items: Seq[JSS] = attributeTree.children.map(x => attributeToJSS(
              x._2,
              SpecialSchemas,
              attributeMap,
              attribute.multiplicity,
              attribute.name ++ List(x._1)
            )).toSeq

            val item: JSS = if (items.size > 1) JSS(Seq(JSA_oneOf(items))) else items.head
            seq.append(JSA_items(
              item
            ))
          }

          // add JSA_required
          if(t.getType().equals(JE_Object)){
            seq.append(JSA_required(
              attributeTree.children.map(x => (x._1.toString,x._2.attribute.multiplicity == attribute.multiplicity)).filter(_._2).map(_._1).toSet
            ))
          }

          JSS(seq)
        })

        if (anyOf.size > 1){
          return JSS(Seq(JSA_oneOf(
            anyOf
          )))
        }
        else return anyOf.head
    }
  }

  private def makeBiMaxJsonSchema(disjointNodes: Types.DisjointNodes,
                                  SpecialSchemas: Map[AttributeName,Types.DisjointNodes],
                                  attributeMap: mutable.HashMap[AttributeName,Attribute],
                                  attributeName: AttributeName
                                 ): JSS = {
    def shred(biMaxNode: BiMaxNode): JSS = {
      attributeToJSS(
        biMaxNodeToAttribute(biMaxNode, attributeMap, attributeName),
        SpecialSchemas,
        attributeMap,
        biMaxNode.multiplicity,
        attributeName
      )
    }

    val anyOf: List[JSS] = disjointNodes.flatMap(x => x.toList).map(shred(_)).toList // no difference between disjoint and combined here
    if (anyOf.size > 1){
      return JSS(Seq(JSA_anyOf(
        anyOf
      )))
    }
    else return anyOf.head
  }

  def biMaxToJsonSchema(SpecialSchemas: Map[AttributeName,Types.DisjointNodes], attributeMap: mutable.HashMap[AttributeName,Attribute]): JSS = {
    // SpecialSchemas is a map of split schemas, this is split on variable_objects and arrays of objects, these should be incorporated back into the schema
    val root = SpecialSchemas.get(mutable.ListBuffer[Any]()).get
    makeBiMaxJsonSchema(root,SpecialSchemas - mutable.ListBuffer[Any](), attributeMap, ListBuffer[Any]())
  }

}
