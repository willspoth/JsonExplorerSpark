package Naive


import BiMax.OurBiMax
import Explorer.{JacksonSerializer, Types}
import Explorer.Types.AttributeName
import JsonExplorer.SparkMain.LogOutput
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KMeans {
  def test(train: RDD[String], validation: RDD[String], log: mutable.ListBuffer[LogOutput], bimax: ListBuffer[(mutable.HashSet[Types.AttributeName], mutable.HashSet[Types.AttributeName])],generateDot: Boolean, k: Int = 3, includeMultiplicities:Boolean = false): (Array[Array[Double]],Array[Array[Double]],Array[Array[Double]]) = {
    var verboseRows: ListBuffer[(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName],Int)] = train.mapPartitions(JacksonSerializer.serialize(_)).map(BiMax.OurBiMax.splitForValidation(_)).aggregate(scala.collection.mutable.HashMap[scala.collection.mutable.HashSet[AttributeName],Int]())(mergeValue,mergeCombiners)
      .map(x => Tuple3(x._1,scala.collection.mutable.HashSet[AttributeName](),x._2)).toList.to[ListBuffer]

    if(!includeMultiplicities)
      verboseRows = verboseRows.map(x => Tuple3(x._1,x._2,math.min(x._3,1)))
    // sync the index of values
    val baseLookup: mutable.HashMap[AttributeName,Int] = verboseRows.foldLeft(mutable.HashMap[AttributeName,Int]()){case(acc,(mand,opt,i)) => {
      (mand++opt).foreach(x => if (!acc.contains(x)) acc.put(x,acc.size))
      acc
    }}
    val baseFVS: Array[Array[Double]] = toFVS(verboseRows.map(x => (x,1)),baseLookup)


    var bimaxOrdered: Array[Array[Double]] = null
    var bimaxLookup: mutable.HashMap[Types.AttributeName, Int] = null
    if(bimax!= null){
      val groupIDs = verboseRows.map(x => x._1 ++ x._2).map(x => BiMax.OurBiMax.groupID(x,bimax)).map(_+1)
      bimaxLookup = bimax.foldLeft(mutable.HashMap[AttributeName,Int]()){case(acc,(mand,opt)) => {
        (mand++opt).foreach(x => if (!acc.contains(x)) acc.put(x,acc.size))
        acc
      }}
      val OracleRows = verboseRows.zip(groupIDs).sortBy(_._2)
      bimaxOrdered = toFVS(verboseRows.zip(groupIDs).sortBy(_._2),bimaxLookup)
      if(generateDot){
        val clusteredValues = verboseRows.zip(groupIDs).sortBy(_._2).foldLeft(mutable.HashMap[Int,(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])]()){case(acc,((mand,opt,i),c)) => {
          acc.get(c) match {
            case Some((m,o)) =>
              val newMand = m.intersect(mand)
              val leftOver = (m -- newMand).union(mand -- newMand)
              val newOpt = opt.union(o).union(leftOver)
              acc.put(c,(newMand,newOpt))
            case None =>
              acc.put(c,(mand,opt))
          }
          acc
        }}.toList.map(_._2).to[ListBuffer]
        makeGraph(clusteredValues,"Ours")
        /*
        val forVerboseOutput = ListBuffer[(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])]()
        for (i <- 1 until 11) {
          forVerboseOutput += Tuple2(verboseRows.zip(groupIDs).filter(_._2 == i).head._1._1,verboseRows.zip(groupIDs).filter(_._2 == i).head._1._1)
        }
        makeGraph(forVerboseOutput, "Verbose")
        */
      }
    }



    val kmeans = smile.clustering.KMeans.lloyd(baseFVS,k)
    val clusterLabels = kmeans.getClusterLabel
    val sortedRows = verboseRows.zip(clusterLabels).sortBy(_._2)

    val clusteredValues = sortedRows.foldLeft(mutable.HashMap[Int,(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])]()){case(acc,((mand,opt,i),c)) => {
      acc.get(c) match {
        case Some((m,o)) =>
          val newMand = m.intersect(mand)
          val leftOver = (m -- newMand).union(mand -- newMand)
          val newOpt = opt.union(o).union(leftOver)
          acc.put(c,(newMand,newOpt))
        case None =>
          acc.put(c,(mand,opt))
      }
      acc
    }}.toList.map(_._2).to[ListBuffer]


    log += LogOutput("UsedK",clusterLabels.distinct.size.toString,"Number of K Used: ")


    val kmeansLookup: mutable.HashMap[AttributeName,Int] = verboseRows.zip(clusterLabels).sortBy(_._2).map(_._1).foldLeft(mutable.HashMap[AttributeName,Int]()){case(acc,(mand,opt,i)) => {
      (mand++opt).foreach(x => if (!acc.contains(x)) acc.put(x,acc.size))
      acc
    }}
    if(bimaxLookup == null)
      bimaxLookup = kmeansLookup
    val kmeansFVS: Array[Array[Double]] = toFVS(verboseRows.zip(clusterLabels.map(_+1)).sortBy(_._2),bimaxLookup)

    // check for merged groups
    /*
    sortedRows.foreach(s => sortedRows.filter(x=> x._2 == s._2 && !x._1.equals(s._1)).foreach(s2 => {
      if((((s2._1._1 ++ s2._1._2).intersect((s._1._1 ++ s._1._2))).size / (math.min((s._1._1 ++ s._1._2).size,(s2._1._1 ++ s2._1._2).size).toDouble)) < 0.5) { // less than 20% in common
        println((s2._1._1 ++ s2._1._2).subsetOf((s._1._1 ++ s._1._2))||(s._1._1 ++ s._1._2).subsetOf((s2._1._1 ++ s2._1._2)))
      }
    }))
    */
    if(generateDot)
      makeGraph(clusteredValues)
    // calculate Precision
    log += LogOutput("KMeansPrecision",clusteredValues.map(x => BigInt(2).pow(x._2.size)).reduce(_+_).toString(),"KMeans Precision: ")
    log += LogOutput("KMeansGrouping",k.toString(),"Number of KMeans Groups: ")
    // calculate Validation
    if(validation.count() > 0) {
      val vali = validation.mapPartitions(x => JacksonSerializer.serialize(x))
        .map(x => OurBiMax.splitForValidation(x))
        .map(x => BiMax.OurBiMax.calculateValidation(x, clusteredValues))
        .reduce(_ + _)
      log += LogOutput("KMeansValidation", ((vali / validation.count().toDouble) * 100.0).toString(), "KMeans Validation: ", "%")
    }

    (baseFVS,kmeansFVS,bimaxOrdered)
  }

  private def mergeValue(s: scala.collection.mutable.HashMap[scala.collection.mutable.HashSet[AttributeName],Int], row: scala.collection.mutable.HashSet[AttributeName]): scala.collection.mutable.HashMap[scala.collection.mutable.HashSet[AttributeName],Int] = {
    val i = s.getOrElse(row,0)
    s.put(row,i+1)
    s
  }

  private def mergeCombiners(s1: scala.collection.mutable.HashMap[scala.collection.mutable.HashSet[AttributeName],Int], s2: scala.collection.mutable.HashMap[scala.collection.mutable.HashSet[AttributeName],Int]): scala.collection.mutable.HashMap[scala.collection.mutable.HashSet[AttributeName],Int] = {
    s1 ++ s2.map{ case (k,v) => k -> (v + s1.getOrElse(k,0)) }
  }

  private def makeGraph(schemas: ListBuffer[(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])], name: String = "KMeans"): Unit = {
    schemas.zipWithIndex
      .foreach(x => Flat.makeDot(Flat.makeGraph(x._1._1++x._1._2),name+"/",x._2.toString))
  }

  def toFVS(rows: ListBuffer[((mutable.HashSet[Types.AttributeName], mutable.HashSet[Types.AttributeName], Int), Int)], lookup: mutable.HashMap[AttributeName,Int]): Array[Array[Double]] = {
    rows.flatMap{case ((mand,opt,count),i) => {
      val buff = mutable.ListBuffer[(mutable.HashSet[Int],mutable.HashSet[Int],Int)]()
      for(x <- 0 until count ){
        val fv: (mutable.HashSet[Int],mutable.HashSet[Int],Int) = Tuple3(mutable.HashSet[Int](),mutable.HashSet[Int](),i)
        mand.foreach(x => fv._1.add(lookup.get(x).get))
        opt.foreach(x => fv._2.add(lookup.get(x).get))
        buff += fv
      }
      buff
    }}.map{case(mand,opt,i) => {
      val fv: Array[Double] =  new Array[Double](lookup.size)
      mand.foreach(x => fv(x) = i.toDouble)
      opt.foreach(x => fv(x) = i.toDouble)
      fv
    }}.toArray
  }
}
