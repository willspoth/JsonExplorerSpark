package util

import Explorer.{Attribute, AttributeTree, GenericTree, JE_Array, JE_Empty_Array, JE_Empty_Object, JE_Obj_Array, JE_Object, JE_Var_Object, JsonExplorerType, Star, Types}
import Explorer.Types.{AttributeName, AttributeNodelet, BiMaxNode, BiMaxStruct}
import Optimizer.RewriteAttributes
import util.JsonSchema.{JSA_additionalProperties, JSA_anyOf, JSA_description, JSA_items, JSA_maxItems, JSA_maxProperties, JSA_oneOf, JSA_properties, JSA_required, JSA_type, JSS, JsonSchemaStructure}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NodeToJsonSchema {

  def biMaxNodeTypeMerger(biMaxNode: BiMaxNode): BiMaxNode = {
    if(biMaxNode.multiplicity > 0) {
      return biMaxNode
    } else {
      BiMaxNode(
        biMaxNode.subsets.flatMap(x => x._1.map(_._1)).toSet, // make schema from subsets
        biMaxNode.subsets.foldLeft(mutable.HashMap[AttributeName,mutable.Set[JsonExplorerType]]()){case(acc,m) => {
          m._1.foreach(x => {
            acc.get(x._1) match {
              case Some(set) => acc.put(x._1,set++x._2)
              case None => acc.put(x._1,x._2)
            }
          })
          acc
        }}.toMap,
        biMaxNode.multiplicity,
        biMaxNode.subsets
      )
    }
  }



  private def biMaxNodeToTree(biMaxNode: BiMaxNode, attributeName: AttributeName, variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)]): GenericTree[AttributeNodelet] = {

    val l: ListBuffer[(Map[AttributeName, mutable.Set[JsonExplorerType]], Int)] =
      if(variableObjWithMult.contains(attributeName))
        (mutable.ListBuffer( (Map((attributeName,(variableObjWithMult.get(attributeName).get._1 ++ mutable.Set(JE_Var_Object)))),variableObjWithMult.get(attributeName).get._2),(biMaxNode.types,biMaxNode.multiplicity)) ++ biMaxNode.subsets)
      else
        (mutable.ListBuffer((biMaxNode.types,biMaxNode.multiplicity)) ++ biMaxNode.subsets)

    val attributeCounts: mutable.HashMap[AttributeName,mutable.HashMap[JsonExplorerType, Int]] = mutable.HashMap[AttributeName,mutable.HashMap[JsonExplorerType, Int]]()
    l.foreach{case(map,mult) => {
      map.foreach{case(n, tSet) => {
        tSet.foreach(t => {
          attributeCounts.get(n) match {
            case Some(m) => m.put(t, m.getOrElse(t, 0) + mult)
            case None => attributeCounts.put(n, mutable.HashMap[JsonExplorerType, Int]((t -> mult)))
          }
        })
      }}
    }}

//      l.flatMap{case(map,mult) =>
//        map.toList.map(x => Tuple2(x,mult))
//      }}.groupBy(_._1).map(x => (x._1,x._2.map(_._2).reduce(_+_)))



    val gTree = RewriteAttributes.GenericListToGenericTree[AttributeNodelet](attributeCounts.toArray.map(x => (x._1,
        AttributeNodelet(x._1,
          mutable.Set[JsonExplorerType](x._2.map(_._1).toSeq:_*),// match {case Some(t) => t case None => {variableObjWithMult.get(attributeName) match {case Some(n) => (n._1++ mutable.Set(JE_Var_Object)) case None => throw new Exception("idk")}}},
          if(x._2.size>0) x._2.map(_._2).reduce(_+_) else 0
        )
      ))
    )

    RewriteAttributes.pickFromTree(attributeName,gTree)
  }

  private def attributeToJSS(
                              tree: GenericTree[AttributeNodelet],
                              SpecialSchemas: Map[AttributeName,Types.DisjointNodes],
                              parentMult: Int,
                              attributeName: AttributeName,
                              pastPayload: AttributeNodelet,
                              variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)]
                            ): JSS = {

    SpecialSchemas.get(attributeName) match {
      case Some(djn) =>
        makeBiMaxJsonSchema(djn,SpecialSchemas-attributeName, attributeName, tree.payload, variableObjWithMult)
      case None =>
        // clean types

        var payload: AttributeNodelet = tree.payload
        if(tree.payload == null)
          payload = pastPayload

        val anyOf = payload.`type`.filter( t => if (t.equals(JE_Empty_Object) && payload.`type`.contains(JE_Object) || (t.equals(JE_Empty_Array) && payload.`type`.contains(JE_Array)) ) false else true)
          .toList.map(t => {
          val seq: ListBuffer[JsonSchemaStructure] = ListBuffer(
            JSA_type(JsonExplorerTypeToJsonSchemaType.convert(t))
          )

          if(t.getType().equals(JE_Object) || t.getType().equals(JE_Var_Object)){
            seq.append(JSA_properties(
              tree.children.map(x => (x._1.toString,attributeToJSS(
                x._2,
                SpecialSchemas,
                payload.multiplicity,
                payload.name ++ List(x._1),
                payload,
                variableObjWithMult
              ))).toMap
            ))
          }

          if(t.getType().equals(JE_Array)){
            val items: Seq[JSS] = tree.children.map(x => attributeToJSS(
              x._2,
              SpecialSchemas,
              payload.multiplicity,
              payload.name ++ List(x._1),
              payload,
              variableObjWithMult
            )).toSeq

            val item: JSS = if (items.size > 1) JSS(Seq(JSA_oneOf(items))) else items.head
            seq.append(JSA_items(
              item
            ))
          }

          // embed entropy for debugging
          //          val description: String = {
          //            List(
          //              attribute.objectTypeEntropy match {
          //                case Some(v) => "ObjectTypeEntropy:" + v.toString
          //                case None => ""
          //              },
          //              attribute.objectMarginalKeySpaceEntropy match {
          //                case Some(v) => "ObjectMarginalKeySpaceEntropy:" + v.toString
          //                case None => ""
          //              },
          //              attribute.objectJointKeySpaceEntropy match {
          //                case Some(v) => "ObjectJointKeySpaceEntropy:" + v.toString
          //                case None => ""
          //              }
          //            ).filter(!_.equals("")).mkString(",")
          //          }
          //
          //          if (description.size > 0) seq.append(JSA_description(description))

          // check for empty arrays
          if (t.getType().equals(JE_Empty_Object)) seq.append(JSA_maxProperties(0.0))
          if (t.getType().equals(JE_Empty_Array)) seq.append(JSA_maxItems(0.0))

          // check for variable objects
          if (t.getType().equals(JE_Var_Object)) seq.append(JSA_additionalProperties(true))

          // TODO array of objects
          if(t.getType().equals(JE_Obj_Array)){
            val items: Seq[JSS] = tree.children.map(x => attributeToJSS(
              x._2,
              SpecialSchemas,
              payload.multiplicity,
              payload.name ++ List(x._1),
              payload,
              variableObjWithMult
            )).toSeq

            val item: JSS = if (items.size > 1) JSS(Seq(JSA_oneOf(items))) else items.head
            seq.append(JSA_items(
              item
            ))
          }

          // add JSA_required
          if(t.getType().equals(JE_Object)){
            seq.append(JSA_required(
              tree.children.map(x => (x._1.toString,x._2.payload.multiplicity == payload.multiplicity)).filter(_._2).map(_._1).toSet
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
                                  attributeName: AttributeName,
                                  pastPayload: AttributeNodelet,
                                  variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)]
                                 ): JSS = {
    def shred(biMaxNode: BiMaxNode): JSS = {
      attributeToJSS(
        biMaxNodeToTree(biMaxNode, attributeName,variableObjWithMult),
        SpecialSchemas,
        biMaxNode.multiplicity,
        attributeName,
        pastPayload,
        variableObjWithMult
      )
    }

    val anyOf: List[JSS] = disjointNodes.flatMap(x => x.toList).map(shred(_)).toList // no difference between disjoint and combined here
    if (anyOf.size > 1){
      return JSS(Seq(JSA_anyOf(
        anyOf
      )))
    }
    else if (anyOf.size == 1) return anyOf.head
    else ???
  }

  def biMaxToJsonSchema(SpecialSchemas: Map[AttributeName,Types.DisjointNodes],variableObjWithMult: Map[AttributeName,(mutable.Set[JsonExplorerType],Int)]): JSS = {
    // SpecialSchemas is a map of split schemas, this is split on variable_objects and arrays of objects, these should be incorporated back into the schema
    val root = SpecialSchemas.get(mutable.ListBuffer[Any]()).get
    makeBiMaxJsonSchema(root,SpecialSchemas - mutable.ListBuffer[Any](), ListBuffer[Any](), null, variableObjWithMult)
  }

}
