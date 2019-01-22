package Viz

import java.io.{StringWriter, Writer}

import Explorer.JsonExtractionSchema
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.io.{ComponentNameProvider, DOTExporter, GraphExporter}

import scala.collection.mutable

object BiMaxViz {
  def viz(schema: JsonExtractionSchema, g: Graph[mutable.HashSet[Int],DefaultEdge]): Unit = {
    val t = schema.attributeLookup.map(x => (x._2,x._1))
    val vertexIdProvider: ComponentNameProvider[mutable.HashSet[Int]] = new ComponentNameProvider[mutable.HashSet[Int]]()
    {
      def getName(set:mutable.HashSet[Int]): String =
      {
        return set.map(t.get(_).get.mkString("_")).mkString("n")
      }
    }

    val vertexLabelProvider: ComponentNameProvider[mutable.HashSet[Int]] = new ComponentNameProvider[mutable.HashSet[Int]]()
    {
      def getName(set:mutable.HashSet[Int]): String =
      {
        return set.map(t.get(_).get.mkString("_")).mkString("\\n")
      }
    }

    val exporter: GraphExporter[mutable.HashSet[Int], DefaultEdge] = new DOTExporter[mutable.HashSet[Int], DefaultEdge](vertexIdProvider, vertexLabelProvider, null)
    val writer: Writer = new StringWriter()
    exporter.exportGraph(g, writer);
    System.out.println(writer.toString());

  }
}
