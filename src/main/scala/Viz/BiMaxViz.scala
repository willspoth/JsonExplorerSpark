//package Viz
//
//
//import Explorer.{JsonExtractionRoot, Types}
//import org.jgrapht.Graph
//import org.jgrapht.graph.DefaultEdge
//
//import scala.collection.mutable
//import scala.collection.JavaConverters._
//import java.io.PrintWriter
//
//import Explorer.Types.SchemaName
//
//
//object BiMaxViz {
//
//  private def cleanSubGraphName(s: SchemaName): String = Types.nameToString(s).replace('.','_').replace('[','_').replace(']','_').replace('*','_')
//
//  private def makeDot(root: JsonExtractionRoot, g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName),DefaultEdge], schemaLookup: mutable.HashMap[SchemaName,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]], schemaIDs: Map[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName),Int]): String = {
//    // create each subgraph
//    val subGraphs = schemaLookup.map(x =>
//      s"""\tsubgraph cluster_${cleanSubGraphName(x._1)}{\n\tlabel="${Types.nameToString(x._1)}"\n"""+
//       s"""${x._2.map{case(mand,opt) => {
//         val t = root.Schemas.get(x._1).get.attributeLookup.map(y => (y._2,y._1))
//         val sorted: List[(String,Int)] = (mand.map(y => (Types.nameToString(t.get(y).get),0)).toList ++ opt.map(y => (Types.nameToString(t.get(y).get),1)).toList).sortBy(_._1)
//         s"""${schemaIDs.get(Tuple3(mand,opt,x._1)).get.toString} [ shape=box label = <<table border="0" cellborder="1" cellspacing="0">\n"""+
//           s"""${sorted.map(x => if(x._2 == 0) "<tr><td><u>"+x._1+"</u></td></tr>" else "<tr><td>"+x._1+"</td></tr>").map("\t\t"+_).mkString("\n")}""" +
//           s"""\n\t\t</table>>];"""
//       }}.mkString("\n")}\n"""+
//      s"""\n\t}"""
//
//    )
//
//    // for every entity, check what outgoing edges it has and connect them
//    val edges: mutable.Set[String]= g.vertexSet().asScala.flatMap{ case(mand,opt,schema) => {
//      val connections: mutable.HashMap[SchemaName,(mutable.HashSet[Int],mutable.HashSet[Int])] = mutable.HashMap[SchemaName,(mutable.HashSet[Int],mutable.HashSet[Int])]()
//      g.edgesOf(Tuple3(mand,opt,schema)).asScala.foreach(e => connections.put(g.getEdgeTarget(e)._3,Tuple2(g.getEdgeTarget(e)._1,g.getEdgeTarget(e)._2))) // only keep one
//      connections.remove(schema) // remove self edge from ?????? library I guess :/
//      connections.map(t => s"""${schemaIDs.get(Tuple3(mand,opt,schema)).get.toString} -- ${schemaIDs.get(Tuple3(t._2._1,t._2._2,t._1)).get.toString} [lhead=cluster_${cleanSubGraphName(t._1)}]""")
//    }}
//
//    s"""graph {\n\tcompound=true\n${subGraphs.mkString("\n")}\n${edges.mkString("\n")}\n}"""
//  }
//
//
//  def viz(name: String, root: JsonExtractionRoot, t: Graph[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName),DefaultEdge], schemaLookup: mutable.HashMap[SchemaName,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]]): Unit = {
//    val schemaIDs: Map[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName),Int] = t.vertexSet().asScala.zipWithIndex.toMap
//    new PrintWriter(name+".dot") { write(makeDot(root,t,schemaLookup,schemaIDs)); close }
//  }
//
//}
//
////dot -Tpdf DotGraphs/root.dot -o DotGraphs/root.pdf