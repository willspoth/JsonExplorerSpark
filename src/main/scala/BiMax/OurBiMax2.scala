package BiMax

import Explorer.JsonExplorerType
import Explorer.Types.{AttributeName, BiMaxNode, BiMaxStruct, DisjointNodes}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OurBiMax2 {

  object setRelation extends Enumeration {
    val subset,combined,disjoint = Value
  }

  private def setRelationship(target: Map[AttributeName,mutable.Set[JsonExplorerType]],
                              test: Map[AttributeName,mutable.Set[JsonExplorerType]]
                             ): setRelation.Value = {
    val t = test.filter(!_._1.isEmpty).map{case(attribute, types) => target.contains(attribute)}
    if(t.reduce(_&&_)){
      return setRelation.subset
    } else {
      if(t.toList.contains(true))
        return setRelation.combined
      else return setRelation.disjoint
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
          case setRelation.subset => subsetSchemas.append(test)
          case setRelation.combined => combinedSchemas.append((test._1,test._2,true))
          case setRelation.disjoint =>
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

  private def isCovered(head: Set[AttributeName], merged: Set[AttributeName]): Boolean = head.subsetOf(merged)

  private def strictSubset(base: Set[AttributeName], toTest: Set[AttributeName]): Boolean = toTest.subsetOf(base) && !toTest.equals(base)

  private def removeSubsets(nodes: BiMaxStruct): BiMaxStruct = {
    def updateIfSubset(target: BiMaxNode, toTest: BiMaxNode): Boolean = {
      if(toTest.schema.subsetOf(target.schema)){
        target.subsets.appendAll(toTest.subsets)
        if(toTest.multiplicity > 0) // filter already merged schemas
          target.subsets.append((toTest.types,toTest.multiplicity))
        return true
      } else {
        return false
      }
    }

    var nodesToCheck = nodes.sortBy(_.schema.size)(Ordering[Int].reverse)
    val returnNodes = ListBuffer[BiMaxNode]()
    while(!nodesToCheck.isEmpty){
      val target = nodesToCheck.head
      nodesToCheck = nodesToCheck.tail.filter(x => !updateIfSubset(target,x)) // negate to remove from pool
      returnNodes.append(target)
    }

    return returnNodes
  }


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
          return (removeSubsets(ListBuffer(new BiMaxNode(testNodes.map(_.schema).reduce(_++_), Map[AttributeName,mutable.Set[JsonExplorerType]](),0,ListBuffer())) ++ nodes),true)
        }
      )
    return (nodes,false) // base case return
  } // if true then need collapse subsets and rerun

  def rewrite(disjointNodes: DisjointNodes
             ): DisjointNodes = {

    def bottomUpRewrite(nodes: BiMaxStruct): (BiMaxStruct,Boolean) = { // boolean should rerun
      nodes.zipWithIndex.foreach{case(test,idx) =>
        val (retNodes,checksubset) = rewriteNode(nodes.takeRight(idx+1))
        if (checksubset)
          return (removeSubsets(nodes.take(nodes.size-(idx+1)) ++ retNodes),true)
      }
      return (nodes,false)
    }

    disjointNodes.map(mixedNodes => {
      var rerun = false
      var tempNodes = mixedNodes.sortBy(_.schema.size)
      do {
        val res = bottomUpRewrite(tempNodes)
        tempNodes = res._1.sortBy(_.schema.size)
        rerun = res._2
      } while(rerun)
      tempNodes
    }).map(removeSubsets(_)) // hit it again just to be sure
  }

}
