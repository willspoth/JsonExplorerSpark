package BiMax


import Explorer._
import Explorer.Types.{AttributeName, SchemaName}
import Viz.BiMaxViz
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.jgrapht._

import scala.collection.JavaConverters._


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


  def buildGraph(root: JsonExtractionRoot, t:Array[(SchemaName,ListBuffer[ListBuffer[entity]])]): (Graph[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName),DefaultEdge],mutable.HashMap[SchemaName,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]]) = {
    val g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName), DefaultEdge] = new DefaultDirectedGraph[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName), DefaultEdge](new DefaultEdge().getClass)
    val schemas: List[SchemaName] = t.toList.map(_._1)
    val lookup: mutable.HashMap[SchemaName,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]] = mutable.HashMap[SchemaName,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]]()

    t.foreach{case(schemaName, disjEntities)=>{
      disjEntities.foreach(n => {
        n.foreach(e => {
          val fields = Tuple3(e.mandatoryAttributes,e.optionalAttributes--e.mandatoryAttributes,schemaName)
          lookup.get(fields._3) match {
            case Some(a) => a += Tuple2(fields._1,fields._2)
            case None => lookup.put(fields._3,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])](Tuple2(fields._1,fields._2)))
          }
          g.addVertex(fields)
        })
      })
    }}
    // find common attributes within groups
    // add entity to subgraph connections
    g.vertexSet().asScala.map { case(manditory,optional,localSchema) => { // for each entity, check if
      val y = root.Schemas.get(localSchema).get.attributeLookup.map(c => (c._2,c._1))
      val attributes: List[AttributeName] = (manditory.map(y.get(_).get).toList ++ optional.map(y.get(_).get).toList)
      schemas.filter(attributes.contains(_)).map(s => {
        lookup.get(s).get.foreach{case(opt,man) => g.addEdge(Tuple3(manditory,optional,localSchema),Tuple3(opt,man,s))}
      })
    }}

    g.vertexSet().asScala.foreach(x=> g.removeEdge(x,x)) // remove self edge

    (g,lookup)
  }


  // start with empty precisionName
  def calculatePrecision(currentSchemaName: SchemaName, g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName),DefaultEdge], entityLookup: mutable.HashMap[SchemaName,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]]): BigInt = {
    val expressions: ListBuffer[BigInt] = entityLookup.get(currentSchemaName).get.map(entity => {
      val v: BigInt = BigInt(2).pow(entity._2.size)
      v*g.edgesOf(Tuple3(entity._1,entity._2,currentSchemaName)).asScala.foldLeft(BigInt(1)){case(acc,edge) => {
        if((!g.getEdgeSource(edge).equals(g.getEdgeTarget(edge))) && g.getEdgeSource(edge)._3.equals(currentSchemaName)){
          acc+calculatePrecision(g.getEdgeTarget(edge)._3,g,entityLookup)
        } else { // ignore self edge
          acc
        }
      }}
    })
    expressions.foldLeft(BigInt(1)){case(acc,v)=> acc+v} // add each entity at this level
  }

  def splitForValidation(row: JsonExplorerType): mutable.HashSet[AttributeName] = {
    val attributeSet: mutable.HashSet[AttributeName] = mutable.HashSet[AttributeName]()

    def extract(name: AttributeName,jet: JsonExplorerType): Unit = {
      jet match {
        case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object => attributeSet += name
        case JE_Object(xs) =>
          xs.foreach(je => extract(name :+ je._1, je._2))
        case JE_Array(xs) =>
          xs.zipWithIndex.foreach(je => {
            extract(name :+ je._2, je._1)
          })
      }
    }

    extract(new AttributeName(), row)
    attributeSet.map(r => r.map(a => {
      a match {
        case _: Int => Star
        case _ => a
      }
    }))
  }

  // manditory, optional
  def graphToSchemaSet(root: JsonExtractionRoot, g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName),DefaultEdge], entityLookup: mutable.HashMap[SchemaName,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]]): mutable.ListBuffer[(mutable.HashSet[AttributeName],mutable.HashSet[AttributeName])] = {
    val possibleSchemas: mutable.ListBuffer[(mutable.HashSet[AttributeName],mutable.HashSet[AttributeName])] = mutable.ListBuffer[(mutable.HashSet[AttributeName],mutable.HashSet[AttributeName])]()

    def createSchema(currentSchemaName: SchemaName, currentAttributeList: (mutable.HashSet[AttributeName],mutable.HashSet[AttributeName]), g: Graph[(mutable.HashSet[Int],mutable.HashSet[Int],SchemaName),DefaultEdge], entityLookup: mutable.HashMap[SchemaName,mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int])]]): Unit = {
      val nameLookup = root.Schemas.get(currentSchemaName).get.attributeLookup.map(_.swap)
      entityLookup.get(currentSchemaName).get.foreach(entity => {
        val mand = entity._1.map(nameLookup.get(_).get)
        val opt = entity._2.map(nameLookup.get(_).get)
        val children: mutable.Set[DefaultEdge] = g.edgesOf(Tuple3(entity._1,entity._2,currentSchemaName)).asScala.filter(edge => (!g.getEdgeSource(edge).equals(g.getEdgeTarget(edge))) && g.getEdgeSource(edge)._3.equals(currentSchemaName))
        if(children.size > 0){ // recursive call
          children.foreach(e => {createSchema(g.getEdgeTarget(e)._3, (currentAttributeList._1 ++ mand,currentAttributeList._2 ++ opt), g, entityLookup)})
        } else { // base case, add to possible schema
          possibleSchemas += Tuple2(currentAttributeList._1 ++ mand,currentAttributeList._2 ++ opt)
        }
      })
    }

    createSchema(ListBuffer[Any](),(mutable.HashSet[AttributeName](),mutable.HashSet[AttributeName]()),g,entityLookup)
    possibleSchemas
  }

  def calculateValidation(attributeSet: mutable.HashSet[AttributeName], possibleSchemas: mutable.ListBuffer[(mutable.HashSet[AttributeName],mutable.HashSet[AttributeName])]): Int = {
    possibleSchemas.foreach{case(mand,opt) => {
      if((mand.subsetOf(attributeSet) || mand.equals(attributeSet)) && ((attributeSet--mand).subsetOf(opt) || (attributeSet--mand).equals(opt)))
        return 1
      else if(attributeSet.subsetOf(mand++opt) || attributeSet.equals(mand++opt))
        return 1
    }}
    return 0
  }

}