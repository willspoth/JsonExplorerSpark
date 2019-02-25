package Naive

import java.io.PrintWriter

import BiMax.OurBiMax
import Explorer.Types.{AttributeName, SchemaName}
import Explorer.{JacksonSerializer, Types}
import JsonExplorer.SparkMain.LogOutput
import org.apache.spark.rdd.RDD
import org.jgrapht.Graph
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object Flat {

  def test(train: RDD[String], validation: RDD[String], log: mutable.ListBuffer[LogOutput], outputDot: Boolean = false): Unit = {
    val flatRows = train.mapPartitions(JacksonSerializer.serialize(_)).map(BiMax.OurBiMax.splitForValidation(_))
    val flatTotalSchema: mutable.HashSet[Types.AttributeName] = flatRows.reduce((l,r) => l.union(r))
    val flatMandatorySchema = flatRows.reduce((l,r) => l.intersect(r))
    val flatOptionalSchema = flatTotalSchema -- flatMandatorySchema
    if(outputDot)
      makeDot(makeGraph(flatTotalSchema))
    // calculate Precision
    log += LogOutput("FlatPrecision",BigInt(2).pow(flatOptionalSchema.size).toString(),"Flat Precision: ")
    // calculate Validation
    val vali = validation.mapPartitions(x => JacksonSerializer.serialize(x)).map(x => OurBiMax.splitForValidation(x)).map(x => BiMax.OurBiMax.calculateValidation(x,ListBuffer(Tuple2(flatMandatorySchema,flatOptionalSchema)))).reduce(_+_)
    log += LogOutput("FlatValidation",((vali/validation.count().toDouble)*100.0).toString(),"Flat Validation: ","%")
  }

  private def makeGraph(schema: mutable.HashSet[Types.AttributeName]): Graph[(Types.AttributeName,mutable.ListBuffer[Types.AttributeName]),DefaultEdge] = {
    val g = new DefaultDirectedGraph[(Types.AttributeName,mutable.ListBuffer[Types.AttributeName]),DefaultEdge](new DefaultEdge().getClass)
    val nodes = schema.foldLeft(mutable.HashMap[Types.AttributeName,mutable.ListBuffer[Types.AttributeName]]()){case(acc,attribute) => {
      var parent = new AttributeName()
      if(attribute.size > 1) {
        parent = attribute.dropRight(1)
      }
      val m = acc.getOrElse(new AttributeName(), mutable.ListBuffer[Types.AttributeName]())
      acc.put(parent,m += attribute)
      acc
    }}
    nodes.foreach(g.addVertex(_))
    val edges = nodes.foreach{case(parent,attributes) => {
      if(parent.size == 0){
        // do nothing
      } else if(parent.size == 1) { // grandparent is root
        g.addEdge((new AttributeName(),nodes.get(new AttributeName()).get),(parent,nodes.get(parent).get))
      } else {
        g.addEdge((parent.dropRight(1),nodes.get(parent.dropRight(1)).get),(parent,nodes.get(parent).get))
      }
    }} // add edges between

    g
  }

  private def makeDot(g: Graph[(Types.AttributeName,mutable.ListBuffer[Types.AttributeName]),DefaultEdge]): Unit = {
    val schemaIDs = g.vertexSet().asScala.zipWithIndex.foldLeft(mutable.HashMap[(Types.AttributeName,mutable.ListBuffer[Types.AttributeName]),Int]()){case(acc,x) => {
      acc.put(x._1,x._2)
      acc
    }}
    val nodes = g.vertexSet().asScala.map{case(parent,attributes) => {
      val sorted = attributes.sortBy(_.toString())
      s"""${schemaIDs.get(Tuple2(parent,attributes)).get.toString} [ shape=box label = <<table border="0" cellborder="1" cellspacing="0">\n"""+
        s"""${sorted.map(x => "<tr><td>"+Types.nameToString(x)+"</td></tr>").map("\t\t"+_).mkString("\n")}""" +
        s"""\n\t\t</table>>];"""
    }}
    val edges = g.edgeSet().asScala.map(edge => {
      val source = g.getEdgeSource(edge)
      val target = g.getEdgeTarget(edge)
      if(source != target && (source._1.size < target._1.size))
        s"""${schemaIDs.get(source).get.toString} -- ${schemaIDs.get(target).get.toString}"""
      else
        null
    }).filter(_ != null)

    new PrintWriter("flat.dot") { write(s"""graph {\n${nodes.mkString("\n")}\n${edges.mkString("\n")}\n}"""); close }
    ???
  }
  //dot -Tpdf flat.dot -o flat.pdf

}
