package BiMax

import Explorer.JsonExplorerType
import Explorer.Types.{AttributeName, BiMaxNode, BiMaxStruct, DisjointNodes}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OurBiMax2 {

  sealed trait setRelation
  case object subset extends setRelation
  case object combined extends setRelation
  case object disjoint extends setRelation

  private def setRelationship(target: Map[AttributeName,mutable.Set[JsonExplorerType]],
                              test: Map[AttributeName,mutable.Set[JsonExplorerType]]
                             ): setRelation = {
    val t = test.map{case(attribute, types) => target.contains(attribute)}
    if(t.reduce(_&&_)){
      return subset
    } else {
      if(t.toList.contains(true))
        return combined
      else return disjoint
    }
  }

  // first split into
  def bin(fvs: mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int]
         ): DisjointNodes = {


    val disjointNodes: mutable.ListBuffer[BiMaxStruct] = mutable.ListBuffer[BiMaxStruct]()

    var remainingNodes: mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int,Boolean)] = fvs.map(x => (x._1,x._2,false)).toList.to[ListBuffer]

    var currentStruct = mutable.ListBuffer[BiMaxNode]()

    // if subset once then never disjoint
    while(!remainingNodes.isEmpty) {
      val subsetSchemas   = mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int,Boolean)]()
      val combinedSchemas = mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int,Boolean)]()
      val disjointSchemas = mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int,Boolean)]()

      val target = remainingNodes.head
      remainingNodes.tail.foreach(test => {
        setRelationship(target._1,test._1) match {
          case subset => subsetSchemas.append(test)
          case combined => combinedSchemas.append((test._1,test._2,true))
          case disjoint =>
            if(test._3) combinedSchemas.append(test)
            else disjointSchemas.append(test)
        }
      })

      currentStruct.append(BiMaxNode(target._1.map(_._1).toSet,target._1,target._2,subsetSchemas.map(x=>(x._1,x._2))))

      if(combinedSchemas.isEmpty) {
        disjointNodes.append(currentStruct)
        currentStruct = mutable.ListBuffer[BiMaxNode]()
      }

      remainingNodes = combinedSchemas ++ disjointSchemas
    }

    disjointNodes
  }

  private def isCovered(base: Set[AttributeName], test: Set[AttributeName]): Boolean = {
    return test.subsetOf(base)
  }

  private def isSubset(test: Set[AttributeName], nodes: BiMaxStruct): Boolean = {
    if(nodes.size < 2)
      return false
    else
      nodes.forall(x => test.subsetOf(x.schema) && !test.equals(x.schema))
  }

  private def removeSubsets(nodes: BiMaxStruct): BiMaxStruct = {
    nodes.filter(x => isSubset(x.schema,nodes))
  }

  //
  private def rewriteNode(nodes: BiMaxStruct): (BiMaxStruct,Boolean) = {
    if(nodes.size > 2) { // check broadest case first to reduce computation
      val temp: Set[AttributeName] = nodes.tail.map(_.schema).reduce(_++_)
      if (!isCovered(nodes.head.schema, temp))
        return (nodes, false)
    }
    val test = nodes.head
    nodes.tail.zipWithIndex.toSet.subsets().toList.filter(_.size > 1).map(_.toList.sortBy(_._2)).sortBy(x => (x.size,x.head._2)).map(_.map(_._1))
      .foreach(testNodes =>
        if(isCovered(
          test.schema,
          testNodes.map(_.schema).reduce(_++_)
        )){
          nodes(0) = new BiMaxNode(testNodes.map(_.schema).reduce(_++_), test.types,0,ListBuffer((test.types,test.multiplicity))++test.subsets)
          return (nodes,true)
        }
      )
    return (nodes,false) // base case return
  } // if true then need collapse subsets and rerun

  def rewrite(disjointNodes: DisjointNodes
             ): DisjointNodes = {

     disjointNodes.map(mixedNodes => {
       mixedNodes.reverse.zipWithIndex.foreach{case(test,idx) =>
         var (retNodes,checksubset) = rewriteNode(mixedNodes.takeRight(idx+1))
         if (checksubset)
           retNodes = removeSubsets(retNodes)
         else

       }
     })
  }

}
