package Viz

import java.io.{File, StringWriter, Writer}

import Explorer.{JsonExtractionSchema, Types}
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.io.{ComponentNameProvider, DOTExporter, GraphExporter}

import scala.collection.mutable
import scala.collection.JavaConverters._

object BiMaxViz {

  // remove the primaryKey/Foreignkeys that only have one edge
  private def removeExtraNodes(g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge]): Unit = {
    val vertexToRemove = mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]()
    g.vertexSet().asScala.foreach(vertex => {
      if(vertex._2 == null){
        if(g.edgesOf(vertex).size() == 1){
          vertexToRemove += vertex
        }
      }
    })
    vertexToRemove.foreach(g.removeVertex(_))
  }

  private def makeDot(schema: JsonExtractionSchema, g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge]): String = {
    val t = schema.attributeLookup.map(x => (x._2,x._1))
    val nodes: List[String] = g.vertexSet().asScala.map{case(manditory,optional) => {
      if(optional == null){ // This is a primary/foreign key node
        s"""${manditory.map(x => Types.nameToFileString(t.get(x).get)).mkString("")} [ label = "${manditory.map(x => Types.nameToString(t.get(x).get)).mkString("\n")}" ];"""
      } else {
        s"""${manditory.map(x => Types.nameToFileString(t.get(x).get)).mkString("")}${optional.map(x => Types.nameToFileString(t.get(x).get)).mkString("")} [ label = "${manditory.map(x => Types.nameToString(t.get(x).get)).mkString("\n")}\n${optional.map(x => Types.nameToString(t.get(x).get)).mkString("\n")}" ];"""
      }
    }}.toList


    val edges: List[String] = g.edgeSet().asScala.map{e => {
      val source: String =
      {
        val s = g.getEdgeSource(e)
        if (s._2 == null)
          s._1.map(x => Types.nameToFileString(t.get(x).get)).mkString("")
        else
          s"""${s._1.map(x => Types.nameToFileString(t.get(x).get)).mkString("")}${s._2.map(x => Types.nameToFileString(t.get(x).get)).mkString("")}"""
      }
      val target: String = {
        val s = g.getEdgeTarget(e)
        if (s._2 == null)
          s._1.map(x => Types.nameToFileString(t.get(x).get)).mkString("")
        else
          s"""${s._1.map(x => Types.nameToFileString(t.get(x).get)).mkString("")}${s._2.map(x => Types.nameToFileString(t.get(x).get)).mkString("")}"""
      }
      s"""${source} --  ${target};"""
    }}.toList

    return "graph {" + nodes.mkString("\n") + edges.mkString("\n") +"}"
  }



  // graph is manditory set then optional set, optional set is null is the flag that it's a primary_key/foreign_key group
  def viz(schema: JsonExtractionSchema, g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge]): Unit = {
    removeExtraNodes(g)
    /*
    val vertexIdProvider: ComponentNameProvider[(mutable.HashSet[Int],mutable.HashSet[Int])] = new ComponentNameProvider[(mutable.HashSet[Int],mutable.HashSet[Int])]()
    {
      def getName(set:(mutable.HashSet[Int],mutable.HashSet[Int])): String =
      {
        if(set._2 == null){
          return set._1.map(t.get(_).get.mkString("_").replace(":", "").replace("-", "")).mkString("n")
        } else {
          return set._1.map(t.get(_).get.mkString("_").replace(":", "").replace("-", "")).mkString("n") + (set._2--set._1).map(t.get(_).get.mkString("_").replace(":", "").replace("-", "")).mkString("n")
        }
      }
    }

    val vertexLabelProvider: ComponentNameProvider[(mutable.HashSet[Int],mutable.HashSet[Int])] = new ComponentNameProvider[(mutable.HashSet[Int],mutable.HashSet[Int])]()
    {
      def getName(set:(mutable.HashSet[Int],mutable.HashSet[Int])): String =
      {
        if(set._2 == null) {
          return set._1.map(t.get(_).get.mkString("_")).mkString("\\n")
        } else {
          return set._1.map(t.get(_).get.mkString("_")).mkString("\\n") + (set._2--set._1).map(t.get(_).get.mkString("_")).mkString("\\n")
        }
      }
    }

    val exporter: GraphExporter[(mutable.HashSet[Int],mutable.HashSet[Int]), DefaultEdge] = new DOTExporter[(mutable.HashSet[Int],mutable.HashSet[Int]), DefaultEdge](vertexIdProvider, vertexLabelProvider, null)
    val writer = new File("DotGraphs/"+Types.nameToFileString(schema.parent)+".dot")
    //val writer: Writer = new StringWriter()
    exporter.exportGraph(g, writer)
    //System.out.println(writer.toString())
*/
    import java.io.PrintWriter
    new PrintWriter("DotGraphs/"+Types.nameToFileString(schema.parent)+".dot") { write(makeDot(schema, g)); close }
  }
}
