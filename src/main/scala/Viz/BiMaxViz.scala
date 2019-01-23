package Viz


import Explorer.{JsonExtractionSchema, Types}
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

import scala.collection.mutable
import scala.collection.JavaConverters._

import java.io.PrintWriter

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
        s"""${manditory.map(x => Types.nameToFileString(t.get(x).get)).mkString("")} [ label = "${manditory.map(x => Types.nameToString(t.get(x).get)).mkString("\\n")}" ];"""
      } else {
        val sorted: List[(String,Int)] = (manditory.map(x => (Types.nameToString(t.get(x).get),0)).toList ++ optional.map(x => (Types.nameToString(t.get(x).get),1)).toList).sortBy(_._1)
        s"""${manditory.map(x => Types.nameToFileString(t.get(x).get)).mkString("")}${optional.map(x => Types.nameToFileString(t.get(x).get)).mkString("")} [ shape=box label = """+
          s"""<<table border="0" cellborder="1" cellspacing="0">\n""" +
          s"""${sorted.map(x => if(x._2 == 0) "<tr><td><u>"+x._1+"</u></td></tr>" else "<tr><td>"+x._1+"</td></tr>").mkString("\\n")}""" +
          s"""\n</table>>];"""

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

    return "graph {\n" + nodes.mkString("\n") + edges.mkString("\n") +"\n}"
  }



  // graph is manditory set then optional set, optional set is null is the flag that it's a primary_key/foreign_key group
  def viz(schema: JsonExtractionSchema, g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge]): Unit = {
    removeExtraNodes(g)
    new PrintWriter("DotGraphs/"+Types.nameToFileString(schema.parent)+".dot") { write(makeDot(schema, g)); close }
  }
}
