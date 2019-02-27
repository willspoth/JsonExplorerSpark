package Naive

import java.awt.{Dimension, GridLayout}

import BiMax.OurBiMax
import Explorer.{JacksonSerializer, Types}
import Explorer.Types.AttributeName
import JsonExplorer.SparkMain.LogOutput
import javax.swing.JFrame
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

    // check for merged groups
    sortedRows.foreach(s => sortedRows.filter(x=> x._2 == s._2 && !x._1.equals(s._1)).foreach(s2 => {
      if((((s2._1._1 ++ s2._1._2).intersect((s._1._1 ++ s._1._2))).size / (math.min((s._1._1 ++ s._1._2).size,(s2._1._1 ++ s2._1._2).size).toDouble)) < 0.5) { // less than 20% in common
        println((s2._1._1 ++ s2._1._2).subsetOf((s._1._1 ++ s._1._2))||(s._1._1 ++ s._1._2).subsetOf((s2._1._1 ++ s2._1._2)))
      }
    }))

    val frame: JFrame = new JFrame("KMeans")
    frame.setPreferredSize(new Dimension(3000, 1500))
    frame.setLayout(new GridLayout(1,2))

    val p1 = smile.plot.heatmap(fvs)
    val p2 = smile.plot.heatmap(fvs.zip(clusterLabels).sortBy(_._2).map(_._1))

    p1.close
    p2.close

    p1.canvas.setTitle("Original")
    p2.canvas.setTitle("KMeans")

    frame.getContentPane.add(p1.canvas)
    frame.getContentPane.add(p2.canvas)

    p1.canvas.reset()
    p2.canvas.reset()

    frame.pack()
    frame.setVisible(true)

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
