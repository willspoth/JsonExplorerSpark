package Naive

import BiMax.OurBiMax
import Explorer.JacksonSerializer
import Explorer.Types.AttributeName
import JsonExplorer.SparkMain.LogOutput
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KMeans {
  def test(train: RDD[String], validation: RDD[String], log: mutable.ListBuffer[LogOutput], k: Int = 5): Unit = {
    val verboseRows: ListBuffer[(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])] = train.mapPartitions(JacksonSerializer.serialize(_)).map(BiMax.OurBiMax.splitForValidation(_)).aggregate(scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]]())(mergeValue,mergeCombiners)
      .map(x => Tuple2(x,scala.collection.mutable.HashSet[AttributeName]())).toList.to[ListBuffer]

    // sync the index of values
    val attributeLookup: mutable.HashMap[AttributeName,Int] = verboseRows.foldLeft(mutable.HashMap[AttributeName,Int]()){case(acc,(mand,opt)) => {
      (mand++opt).foreach(x => if (!acc.contains(x)) acc.put(x,acc.size))
      acc
    }}
    val fvs: Array[Array[Double]] = verboseRows.map{case(mand,opt) => {
      val fv: (mutable.HashSet[Int],mutable.HashSet[Int]) = Tuple2(mutable.HashSet[Int](),mutable.HashSet[Int]())
      mand.foreach(x => fv._1.add(attributeLookup.get(x).get))
      opt.foreach(x => fv._2.add(attributeLookup.get(x).get))
      fv
    }}.map{case(mand,opt) => {
      val fv: Array[Double] =  new Array[Double](attributeLookup.size)
      mand.foreach(x => fv(x) = 1.0)
      opt.foreach(x => fv(x) = 1.0)
      fv
    }}.toArray

    val kmeans = smile.clustering.KMeans.lloyd(fvs,k)
    val clusterLabels = kmeans.getClusterLabel
    val sortedRows = verboseRows.zip(clusterLabels).sortBy(_._2)

    val clusteredValues = sortedRows.foldLeft(mutable.HashMap[Int,(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])]()){case(acc,((mand,opt),c)) => {
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

    makeGraph(clusteredValues)
    // calculate Precision
    log += LogOutput("KMeansPrecision",clusteredValues.map(x => BigInt(2).pow(x._2.size)).reduce(_+_).toString(),"KMeans Precision: ")
    // calculate Validation
    if(validation.count() > 0) {
      val vali = validation.mapPartitions(x => JacksonSerializer.serialize(x))
        .map(x => OurBiMax.splitForValidation(x))
        .map(x => BiMax.OurBiMax.calculateValidation(x, clusteredValues))
        .reduce(_ + _)
      log += LogOutput("KMeansValidation", ((vali / validation.count().toDouble) * 100.0).toString(), "KMeans Validation: ", "%")
    }
    ???
  }

  private def mergeValue(s: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]], row: scala.collection.mutable.HashSet[AttributeName]): scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]] = {
    s += row
    s
  }

  private def mergeCombiners(s1: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]], s2: scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]]): scala.collection.mutable.HashSet[scala.collection.mutable.HashSet[AttributeName]] = {
    s1 ++= s2
  }

  private def makeGraph(schemas: ListBuffer[(mutable.HashSet[AttributeName], mutable.HashSet[AttributeName])]): Unit = {
    schemas.zipWithIndex
      .foreach(x => Flat.makeDot(Flat.makeGraph(x._1._1++x._1._2),"KMeans/",x._2.toString))
  }
}
