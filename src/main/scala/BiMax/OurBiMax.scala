package BiMax

import Extractor.{FeatureVector, JsonExplorerType}
import Extractor.Types.{AttributeName, BiMaxNode, BiMaxStruct, DisjointNodes}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object OurBiMax {

//  val logger = Logger.getLogger(this.getClass)

  object setRelation extends Enumeration {
    val subset,combined,disjoint = Value
  }

  private def setRelationship(target: Set[AttributeName],
                              test: Set[AttributeName]
                             ): setRelation.Value = {

    val testClean = test.filterNot(_.isEmpty)
    if(testClean.subsetOf(target)){
      return setRelation.subset
    } else {
      testClean.foreach(attr => if(target.contains(attr)) return setRelation.combined)
      return setRelation.disjoint
    }
  }

  // first split into
  def bin(fvs: mutable.HashMap[Map[AttributeName,mutable.Set[JsonExplorerType]],Int]
         ): DisjointNodes = {


    val disjointNodes: mutable.ListBuffer[BiMaxStruct] = mutable.ListBuffer[BiMaxStruct]()

    var remainingNodes: mutable.ListBuffer[(Set[AttributeName],Map[AttributeName,mutable.Set[JsonExplorerType]],Int,Boolean)] = fvs.map(x => (x._1.map(_._1).toSet,x._1,x._2,false)).toList.to[ListBuffer].sortBy(_._2.size)(Ordering[Int].reverse)

    var currentStruct = mutable.ListBuffer[BiMaxNode]()

    // if subset once then never disjoint
    while(!remainingNodes.isEmpty) {
      val subsetSchemas   = mutable.ListBuffer[(Set[AttributeName],Map[AttributeName,mutable.Set[JsonExplorerType]],Int,Boolean)]()
      val combinedSchemas = mutable.ListBuffer[(Set[AttributeName],Map[AttributeName,mutable.Set[JsonExplorerType]],Int,Boolean)]()
      val disjointSchemas = mutable.ListBuffer[(Set[AttributeName],Map[AttributeName,mutable.Set[JsonExplorerType]],Int,Boolean)]()

      val target = remainingNodes.head
      remainingNodes.tail.foreach(test => {
        setRelationship(target._1,test._1) match {
          case setRelation.subset => subsetSchemas.append(test)
          case setRelation.combined => combinedSchemas.append((test._1,test._2,test._3,true))
          case setRelation.disjoint =>
            if(test._4) combinedSchemas.append(test)
            else disjointSchemas.append(test)
        }
      })

      currentStruct.append(BiMaxNode(target._1,target._2,target._3,subsetSchemas.map(x=>(x._2,x._3))))

      if(combinedSchemas.isEmpty) {
        disjointNodes.append(currentStruct)
        currentStruct = mutable.ListBuffer[BiMaxNode]()
      }

      remainingNodes = combinedSchemas ++ disjointSchemas
    }

    disjointNodes
  }

  private def isCovered(head: Set[AttributeName], merged: Set[AttributeName]): Boolean = {
    head.subsetOf(merged)
  }

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

    //logger.debug(("\t"*4) + "RemovingSubsets")
    var nodesToCheck = nodes.sortBy(_.schema.size)(Ordering[Int].reverse)
    val returnNodes = ListBuffer[BiMaxNode]()
    while(!nodesToCheck.isEmpty){
      val target = nodesToCheck.head
      nodesToCheck = nodesToCheck.tail.filter(x => !updateIfSubset(target,x)) // negate to remove from pool
      returnNodes.append(target)
    }
    //logger.debug(("\t"*4) + "Done Removing Subsets")
    return returnNodes
  }


  private def rewriteNode(head: BiMaxNode, childrenNodes: BiMaxStruct, fast: Boolean): (BiMaxStruct,Boolean) = {
//    logger.debug(("\t"*3) + "Rewritting Node")

      val sets = {
       if(fast) childrenNodes.toSet.subsets(childrenNodes.size-1)
       else childrenNodes.toSet.subsets()
      }
      sets.filter(_.size > 0)
        .foreach(testNodes => {
          //        logger.debug(("\t"*3) + "Testing Node with combination size: " + testNodes.size)
          val tailSchema = testNodes.map(_.schema).reduce(_ ++ _)
          if (isCovered(
            head.schema,
            tailSchema
          )) {
            //logger.debug(("\t"*3) + "Done Rewritting Node")
            return (removeSubsets(ListBuffer(new BiMaxNode(tailSchema, Map[AttributeName, mutable.Set[JsonExplorerType]](), 0, ListBuffer())) ++ childrenNodes ++ ListBuffer[BiMaxNode](head)), true)
          }
        })

    //logger.debug(("\t"*3) + "Done Rewritting Node")
    return (childrenNodes,false) // base case return
  } // if true then need collapse subsets and rerun

  def rewrite(disjointNodes: DisjointNodes, fast: Boolean): DisjointNodes = {

    def bottomUpRewrite(nodes: BiMaxStruct): (BiMaxStruct,Boolean) = { // boolean should rerun
      nodes.zipWithIndex.foreach{case(test,idx) =>
        if(idx > 2) {
//          logger.debug(("\t"*1) + "Testing Node idx: " + idx.toString)
          val childNodes = nodes.take(idx)
          if(isCovered(test.schema, childNodes.map(_.schema).reduce(_ ++ _))) {
            val (retNodes, checksubset) = rewriteNode(test,childNodes, fast) // take not inclusive, take(1) returns for element
            if (checksubset)
              return (removeSubsets(nodes.takeRight(nodes.size - (idx + 1)) ++ retNodes), true)
          }
        }
      }
      return (nodes,false)
    }

    disjointNodes.map(mixedNodes => {
      var rerun = false
      var tempNodes = mixedNodes.sortBy(_.schema.size)//(Ordering[Int].reverse)
      do {
//        if(rerun)
//          logger.debug(("\t"*0) + "Rerunning BiMax")
//        else
//          logger.debug(("\t"*0) + "Running BiMax")
        val res = bottomUpRewrite(tempNodes)
        tempNodes = res._1.sortBy(_.schema.size)//(Ordering[Int].reverse)
        rerun = res._2
      } while(rerun)
      tempNodes
    }).map(removeSubsets(_)) // hit it again just to be sure
  }

}
