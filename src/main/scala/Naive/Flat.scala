package Naive


import Explorer.Types.{AttributeName, BiMaxNode, BiMaxStruct, DisjointNodes}
import Explorer.{Attribute, Extract, JacksonShredder, JsonExplorerType}
import JsonExplorer.SparkMain.LogOutput
import org.apache.spark.rdd.RDD

import scala.collection.mutable

// motivation -> create ground truth from documentation page, show how bad validation is

object Flat {

  def generate(train: RDD[String], log: mutable.ListBuffer[LogOutput]): Unit = {

    // TODO

    val flattenedAttributes: Array[(AttributeName,Attribute)] = train.mapPartitions(JacksonShredder.shred(_)).flatMap(Extract.ExtractAttributes(_))
      .combineByKey(Extract.createCombiner,Extract.mergeValue,Extract.mergeCombiners)
      .map{case(n,t) => {
        (n,Attribute(n,t))
      }}.collect()

    val attributesMolded: mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int)] =
      flattenedAttributes.map(x => {
        (Map[AttributeName,mutable.Set[JsonExplorerType]](x._1->x._2.`type`),
          x._2.multiplicity
        )
      }).toList.to[mutable.ListBuffer]

    val root: Map[AttributeName,DisjointNodes] = Map(
      (mutable.ListBuffer[Any](), // root
        mutable.Seq[BiMaxStruct]( // DisjointNode
          mutable.Seq[BiMaxNode]( // BiMaxStruct, make all attributes a node
            BiMaxNode(
              Set[AttributeName](),
              Map[AttributeName,mutable.Set[JsonExplorerType]](),
              0,
              attributesMolded
            )
          )
        )
      )
    )

    val m = mutable.HashMap[AttributeName,Attribute](flattenedAttributes: _*)
    val JsonSchema: util.JsonSchema.JSS = util.NodeToJsonSchema.biMaxToJsonSchema(root, m)
    val JsonSchemaString = JsonSchema.toString
    println(JsonSchemaString)
  }


}
