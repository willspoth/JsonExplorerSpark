package BiMax


import Explorer.{Attribute, JsonExtractionSchema}
import Explorer.Types.{AttributeName, SchemaName}
import Viz.BiMaxViz
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge, DefaultUndirectedGraph}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object OurBiMax {

  // sparse encoding, multiplicity, alwaysBeenDisjoint
  private type Row = (mutable.HashSet[Int],Int,Boolean) // this is the sparse encoding of the row
  case class BiMaxNode(root: Row, subset: ListBuffer[Row], combinedset: ListBuffer[Row], disjointset: ListBuffer[Row], remainder: Option[BiMaxNode])

  sealed trait Relationship
  case object subset extends Relationship
  case object combinedset extends Relationship
  case object disjointset extends Relationship


  private def relationship(parent: Row, child: Row): Relationship = {
    // can be made faster
    if(child._1.subsetOf(parent._1))
      return subset
    else if (parent._1.intersect(child._1).nonEmpty)
      return combinedset
    else
      return disjointset
  }

  private def rowComputation(rows: ListBuffer[Row]): (Row,ListBuffer[Row],ListBuffer[Row],ListBuffer[Row]) = {
    val maxRow = (rows.head._1,rows.head._2,false)
    val (sub,com,dis) = rows.tail.foldLeft(ListBuffer[Row](),ListBuffer[Row](),ListBuffer[Row]()){case((sub,com,dis),x) =>
      relationship(maxRow,x) match {
        case _:subset.type => sub += Tuple3(x._1,x._2,false)
        case _:combinedset.type => com += Tuple3(x._1,x._2,false)
        case _:disjointset.type => dis += Tuple3(x._1,x._2,x._3 && true)
      }
      (sub,com,dis)
    }
    return Tuple4(maxRow,sub,com,dis)
  }


  private def biMaxTailComputation(comIn: ListBuffer[Row], disjIn: ListBuffer[Row], BiMaxList: ListBuffer[BiMaxNode]): Option[BiMaxNode] = {
    if(comIn.isEmpty) {
      biMaxHeadComputation(disjIn, BiMaxList)
      return None
    }

    //val rows: ListBuffer[Row] = (comIn ++ disjIn).sortBy(x => (x._3,-x._2))
    val rows: ListBuffer[Row] = (comIn ++ disjIn).sortBy(x => (x._3,-x._1.size))
    val (maxRow,sub,com,dis) = rowComputation(rows)

    return Some(BiMaxNode(maxRow, sub, com, dis, biMaxTailComputation(com,dis,BiMaxList)))
  }

  // preforms greedy algorithm on some list of rows,
  private def biMaxHeadComputation(in: ListBuffer[Row], BiMaxList: ListBuffer[BiMaxNode]): Unit = {
    if(in.isEmpty) // sanity check
      return // don't add anything

    //val rows: ListBuffer[Row] = in.sortBy(-_._2) // descending order
    val rows: ListBuffer[Row] = in.sortBy(-_._1.size) // descending order
    val (maxRow,sub,com,dis) = rowComputation(rows)

    BiMaxList += BiMaxNode(maxRow, sub, com, dis, biMaxTailComputation(com,dis,BiMaxList))

  }


  def BiMax(schemaName: SchemaName, m: scala.collection.mutable.HashMap[ArrayBuffer[Byte], Int]): (SchemaName,ListBuffer[BiMaxNode]) = {
    val BiMaxList: ListBuffer[BiMaxNode] = ListBuffer[BiMaxNode]()
    if(m.isEmpty)
      return Tuple2(schemaName,BiMaxList)

    val rows: ListBuffer[Row] = m.map(x => (x._1.zipWithIndex.flatMap(y => if (y._1 != 0) List[Int](y._2) else List[Int]()).to[mutable.HashSet],x._2,true)).to[ListBuffer]
    biMaxHeadComputation(rows, BiMaxList)
    return Tuple2(schemaName,BiMaxList)
  }


  // now rewrite the tree based on our heuristic
  // ListOfDisjointNodes[ListOfNodes[(CombinedRow,ContributingRows)]]
  type RewrittenBiMaxNode = ListBuffer[ListBuffer[(Row,ListBuffer[Row])]] // (unionedRow,Rows that contributed to the union)

  // merges all rows together
  private def combine(rbn: RewrittenBiMaxNode): RewrittenBiMaxNode = {
    val r: ListBuffer[(Row,ListBuffer[Row])] = rbn.foldLeft(ListBuffer[(Row,ListBuffer[Row])]()){case(acc,pairs) => {
      pairs.foldLeft(acc){case(acc,p) => acc += p}
    }}
    val newRBN = new RewrittenBiMaxNode()
    newRBN += r
    return newRBN
  }
  private def unionNodes(nodes: RewrittenBiMaxNode): mutable.HashSet[Int] = return nodes.foldLeft(mutable.HashSet[Int]()){case(acc,x) => acc.union(unionSet(x))}
  private def unionSet(allSets: ListBuffer[(Row,ListBuffer[Row])]): mutable.HashSet[Int] = return allSets.foldLeft(mutable.HashSet[Int]()){case(acc,(r,x)) => acc.union(r._1)}


  private def collapse(in: RewrittenBiMaxNode): RewrittenBiMaxNode = {
    val r = new RewrittenBiMaxNode()
    r += in.head
    val (rbnAcc,_,hashAcc) = in.tail.foldLeft(r, unionSet(in.head), new mutable.HashSet[Int]()){case((rbnAcc,hashCurr,hashAcc),(pairs)) => {
      if(hashCurr.subsetOf(hashAcc.union(unionSet(pairs)))){ // combine all and update
        rbnAcc += pairs
        val newRBNAcc = combine(rbnAcc)
        (newRBNAcc,unionNodes(newRBNAcc),mutable.HashSet[Int]())
      } else {
        rbnAcc += pairs
        (rbnAcc,hashCurr,hashAcc.union(unionSet(pairs)))
      }
    }}
    return rbnAcc
  }


  private def convertBiMaxNode(rewrittenBiMaxNode: RewrittenBiMaxNode): RewrittenBiMaxNode = {
    val checkedNodes: RewrittenBiMaxNode = new RewrittenBiMaxNode()
    var currentRBN = rewrittenBiMaxNode

    do {
      val nextRBN = collapse(currentRBN)
      checkedNodes += nextRBN.head
      currentRBN = nextRBN.tail
    } while(currentRBN.nonEmpty)

    return checkedNodes
  }


  def convertBiMaxNodes(schemaName: SchemaName, biMaxList: ListBuffer[BiMaxNode]): (SchemaName, ListBuffer[RewrittenBiMaxNode]) = {
    val rbn: ListBuffer[RewrittenBiMaxNode] = biMaxList.map(x => {
      val l = new RewrittenBiMaxNode()
      var current: BiMaxNode = x
      while(current != null){
        l += ListBuffer(Tuple2(current.root,current.subset))
        current.remainder match {
          case Some(v) => current = v
          case None => current = null
        }
      }
      l.reverse
    }).map(convertBiMaxNode(_))
    (schemaName, rbn)
  }

  // finally apply the visualization heuristic

  case class entity(mandatoryAttributes: mutable.HashSet[Int], optionalAttributes: mutable.HashSet[Int], featureVectors: ListBuffer[(mutable.HashSet[Int],Int)], combinedMult: Int)

  // list of disjoint sets
  def categorizeAttributes(schemaName: SchemaName, RBNS: ListBuffer[RewrittenBiMaxNode]): (SchemaName, ListBuffer[ListBuffer[entity]]) = {
    val entities: ListBuffer[ListBuffer[entity]] = RBNS.map(rbn => { // rbn is a list of nodes
      rbn.map( node => {
        val (mandatory, optional, coll, combinedMult) = node.foldLeft(node.head._1._1.clone(),node.head._1._1.clone(),ListBuffer[(mutable.HashSet[Int],Int)](),0){case((mandatory, optional, coll, combinedMult),(maxNode, nodeSet)) => {
          (nodeSet :+ maxNode).foldLeft(mandatory,optional,coll,combinedMult){case((m,opt,coll,mult),v)=> {
            if(v._1.isEmpty) {
              (m,opt,coll,mult)
            } else {
              coll += Tuple2(v._1, v._2)
              (v._1.intersect(m), v._1.union(opt), coll, mult + v._2)
            }
          }}
        }}
        new entity(mandatory,optional,coll, combinedMult)
      })
    })
    (schemaName,entities)
  }

  import org.jgrapht._
  def buildGraph(schemaName: SchemaName, disjEntities: ListBuffer[ListBuffer[entity]]): (SchemaName, Graph[(mutable.HashSet[Int],mutable.HashSet[Int]),DefaultEdge]) = {
    val g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int]), DefaultEdge] = new DefaultUndirectedGraph[(mutable.HashSet[Int],mutable.HashSet[Int]), DefaultEdge](new DefaultEdge().getClass)
    disjEntities.foreach(n => {
      n.foreach(e => {
        val fields = Tuple2(e.mandatoryAttributes,e.optionalAttributes--e.mandatoryAttributes)
        g.addVertex(fields)
        e.mandatoryAttributes.foreach(i => {
          val s = new mutable.HashSet[Int]()
          s += i
          if(!g.containsVertex(Tuple2(s,null))){
            g.addVertex(Tuple2(s,null))
          }
          g.addEdge(Tuple2(s,null),fields)
        })
      })
    })
    (schemaName,g)
  }


  def test(): Unit = {
    val testMatrix: mutable.HashMap[mutable.ArrayBuffer[Byte],Int] =  mutable.HashMap[mutable.ArrayBuffer[Byte],Int]()
    testMatrix.put(mutable.ArrayBuffer[Byte](1,1,1,0,0),5)
    testMatrix.put(mutable.ArrayBuffer[Byte](1,0,1,0,0),4)
    testMatrix.put(mutable.ArrayBuffer[Byte](0,0,0,0,0),3)
    testMatrix.put(mutable.ArrayBuffer[Byte](0,0,0,1,1),2)



    val (r,r1) = OurBiMax.BiMax(ListBuffer[Any]("test"),testMatrix)
    val r2 = OurBiMax.convertBiMaxNodes(r,r1)
    val r3 = OurBiMax.categorizeAttributes(r2._1,r2._2)
    val r4 = OurBiMax.buildGraph(r3._1,r3._2)

    val attributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute] = scala.collection.mutable.HashMap[AttributeName,Attribute]()
    val schema = new JsonExtractionSchema()
    schema.parent = ListBuffer[Any]("test")
    schema.attributeLookup = new scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Int]()
    schema.attributeLookup.put(ListBuffer[Any]("a"),0)
    schema.attributeLookup.put(ListBuffer[Any]("b"),1)
    schema.attributeLookup.put(ListBuffer[Any]("c"),2)
    schema.attributeLookup.put(ListBuffer[Any]("d"),3)
    schema.attributeLookup.put(ListBuffer[Any]("e"),4)

    BiMaxViz.viz(schema, r4._2)

    println("done")
  }

}