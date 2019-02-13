package Viz


import Explorer.{JsonExtractionRoot, JsonExtractionSchema, Types}
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

import scala.collection.mutable
import scala.collection.JavaConverters._
import java.io.PrintWriter

import Explorer.Types.{AttributeName, SchemaName}

import scala.collection.mutable.ListBuffer


class BiMaxViz {

  val lookupSchemas: mutable.HashMap[(ListBuffer[Any],(mutable.HashSet[Int],mutable.HashSet[Int])),Int] = mutable.HashMap[(ListBuffer[Any],(mutable.HashSet[Int],mutable.HashSet[Int])),Int]() // this will be a global identifier for the hashset
  val GID: mutable.HashMap[ListBuffer[Any],mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]] = mutable.HashMap[ListBuffer[Any],mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]]()


  //dot -Tpdf DotGraphs/root.dot -o DotGraphs/root.pdf
  // remove the primaryKey/Foreignkeys that only have one edge
  private def removeExtraNodes(g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge]): Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge] = {
    val vertexToRemove = mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]()
    g.vertexSet().asScala.foreach(vertex => {
      if(vertex._2 == null){
        if(g.edgesOf(vertex).size() == 1){
          vertexToRemove += vertex
        }
      }
    })
    vertexToRemove.foreach(g.removeVertex(_))
    g
  }

  private def makeDot(schema: JsonExtractionSchema, g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge]): String = {
    val t = schema.attributeLookup.map(x => (x._2,x._1))
    val nodes: List[String] = g.vertexSet().asScala.map{case(manditory,optional) => {
      if(optional == null){ // This is a primary/foreign key node
        s"""${lookupSchemas.get((schema.parent,(manditory,optional))).get.toString} [ label = "${manditory.map(x => Types.nameToString(t.get(x).get)).mkString("\\n")}" ];"""
      } else {
        val sorted: List[(String,Int)] = (manditory.map(x => (Types.nameToString(t.get(x).get),0)).toList ++ optional.map(x => (Types.nameToString(t.get(x).get),1)).toList).sortBy(_._1)
        GID.get(schema.parent) match {
          case Some(l) => l += Tuple2(manditory,optional)
          case None => GID.put(schema.parent,ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]((manditory,optional)))
        }
        //s"""${manditory.map(x => Types.nameToFileString(t.get(x).get)).mkString("")}${optional.map(x => Types.nameToFileString(t.get(x).get)).mkString("")}"""
        s"""${lookupSchemas.get((schema.parent,(manditory,optional))).get.toString} [ shape=box label = """+
          s"""<<table border="0" cellborder="1" cellspacing="0">\n""" +
          s"""${sorted.map(x => if(x._2 == 0) "<tr><td><u>"+x._1+"</u></td></tr>" else "<tr><td>"+x._1+"</td></tr>").map("\t\t"+_).mkString("\n")}""" +
          s"""\n\t\t</table>>];"""

      }
    }}.toList

    // finds edges within each subgroup
    val edges: List[String] = g.edgeSet().asScala.map{e => {
      val source: String =
      {
        val s = g.getEdgeSource(e)
        lookupSchemas.get((schema.parent,(s._1,s._2))).get.toString
      }
      val target: String = {
        val s = g.getEdgeTarget(e)
        lookupSchemas.get((schema.parent,(s._1,s._2))).get.toString
      }
      s"""${source} --  ${target};"""
    }}.toList

    return s"""\tsubgraph cluster_${cleanSubGraphName(schema.parent)} {\n\tlabel="${Types.nameToString(schema.parent)}"\n${nodes.map("\t\t"+_).mkString("\n")}\n\n${edges.map("\t\t"+_).mkString("\n")}\n\t}"""
  }

  private def cleanSubGraphName(s: SchemaName): String = Types.nameToString(s).replace('.','_').replace('[','_').replace(']','_').replace('*','_')

  def findConnectingGroups(root: JsonExtractionRoot, t: (SchemaName, Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge]), schemas: Array[AttributeName]): ListBuffer[String] = {
    val edges: ListBuffer[String] = ListBuffer[String]()

    t._2.vertexSet().asScala.map { case(manditory,optional) => { // for each entity, check if
      val y = root.Schemas.get(t._1).get.attributeLookup.map(c => (c._2,c._1))
      if(optional != null){
        val attributes: List[AttributeName] = (manditory.map(y.get(_).get).toList ++ optional.map(y.get(_).get).toList)
        schemas.filter(attributes.contains(_)).map(s => {
          edges += s"""${lookupSchemas.get((root.Schemas.get(t._1).get.parent,(manditory,optional))).get.toString} -- ${lookupSchemas.get((s,GID.get(s).get.head)).get.toString} [lhead=cluster_${cleanSubGraphName(s)}]"""
        })
      }
    }}
    edges
  }

  def populate(t: Array[(SchemaName, Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge])]): Unit = {
    t.foldLeft(0){ case (idx, (schema, g)) => {
      g.vertexSet().asScala.foldLeft(idx) { case (localIdx, (mand, opt)) =>{
        lookupSchemas.put((schema, (mand, opt)), localIdx)
        localIdx + 1
      }}
    }}
  }


  def viz(name: String, root: JsonExtractionRoot, t: Array[(SchemaName, Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge])]): Unit = {
    populate(t)
    val innerDot: Array[String] = t.map(x => {makeDot(root.Schemas.get(x._1).get, removeExtraNodes(x._2))})
    // now connect each schema to each parent sub component
    val connectingGroups: Array[String] = t.map(x=>findConnectingGroups(root,x,t.map(_._1))).flatten // look for each subgraph, find where it connects
    new PrintWriter(name+".dot") { write(s"""graph g {\n\tcompound=true\n${innerDot.mkString("\n\n")}\n${connectingGroups.mkString("\n")}\n}"""); close }
  }
}
