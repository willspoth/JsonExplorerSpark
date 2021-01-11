package Exec

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.clustering.{BisectingKMeansModel, KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import Extractor.JsonExplorerType
import Extractor.Types.{AttributeName, BiMaxNode, DisjointNodes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable


object Algorithms {

  def toDisjointNodes(m: scala.collection.mutable.HashMap[Map[AttributeName, scala.collection.mutable.Set[JsonExplorerType]], Int]): DisjointNodes = {
    val a: mutable.Seq[BiMaxNode] = m.map(x => {
      BiMaxNode(
        x._1.map(_._1).toSet,
        x._1,
        x._2,
        mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int)]()
      )
    }).toSeq.to[mutable.Seq]

    return scala.collection.mutable.Seq(
      a
    )
  }


  def runKMeans(sc: SparkContext, disjointNodes: DisjointNodes, k: Int): DisjointNodes = {
    val schema = mutable.HashMap[AttributeName,Int]()
    if(disjointNodes.size > 1)
      ??? // assert for this hack job

    disjointNodes.head.foreach(fv => fv.schema.foreach(x => if(!schema.contains(x)) schema.put(x,schema.size+1)))

    val sparseFVS: Array[org.apache.spark.mllib.linalg.Vector] = disjointNodes.head.map(fv => fv.schema.map(schema.get(_).get).toArray.sorted(Ordering.Int)).toArray.map(sparseVec => {
      Vectors.sparse(schema.size,sparseVec.map(asd => (asd-1,1.0)))
    })

    val fvs: RDD[org.apache.spark.mllib.linalg.Vector] = sc.parallelize(sparseFVS)

    val clusters: KMeansModel = KMeans.train(fvs, k, 20)

    val rows: mutable.Seq[(BiMaxNode,Int)] = disjointNodes.head.zip(clusters.predict(fvs).collect())

    val groups: mutable.Seq[BiMaxNode] = rows.groupBy(_._2).map(_._2.map(_._1)).map(x => {
      // combine nodes
      val localSchema = mutable.Set[AttributeName]()
      val types = mutable.Map[AttributeName,mutable.Set[JsonExplorerType]]()
      var mult: Int = 0
      var subsets = mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int)]()

      x.foreach(bmn => {
        bmn.schema.foreach(localSchema.add(_))
        bmn.types.map{case(name,localTypes) => {
          types.get(name) match {
            case Some(groupTypes) => localTypes.foreach(groupTypes.add(_))
            case None => types.put(name,localTypes)
          }
        }}
        mult += bmn.multiplicity
        subsets.append((bmn.types,bmn.multiplicity))
      })

      BiMaxNode(
        localSchema.toSet,
        types.toMap,
        mult,
        subsets
      )
    }).toSeq.to[mutable.Seq]

    return mutable.Seq(groups)
  }

  def runHierachical(sc: SparkContext, disjointNodes: DisjointNodes, k: Int): DisjointNodes = {
    val schema = mutable.HashMap[AttributeName,Int]()
    if(disjointNodes.size > 1)
      ??? // assert for this hack job

    disjointNodes.head.foreach(fv => fv.schema.foreach(x => if(!schema.contains(x)) schema.put(x,schema.size+1)))

    val sparseFVS: Array[org.apache.spark.mllib.linalg.Vector] = disjointNodes.head.map(fv => fv.schema.map(schema.get(_).get).toArray.sorted(Ordering.Int)).toArray.map(sparseVec => {
      Vectors.sparse(schema.size,sparseVec.map(asd => (asd-1,1.0)))
    })

    val fvs: RDD[org.apache.spark.mllib.linalg.Vector] = sc.parallelize(sparseFVS)

    val model = new BisectingKMeans().setK(k).setSeed(1l)

    val clusters = model.run(fvs)

    val rows: mutable.Seq[(BiMaxNode,Int)] = disjointNodes.head.zip(clusters.predict(fvs).collect())

    val groups: mutable.Seq[BiMaxNode] = rows.groupBy(_._2).map(_._2.map(_._1)).map(x => {
      // combine nodes
      val localSchema = mutable.Set[AttributeName]()
      val types = mutable.Map[AttributeName,mutable.Set[JsonExplorerType]]()
      var mult: Int = 0
      var subsets = mutable.ListBuffer[(Map[AttributeName,mutable.Set[JsonExplorerType]],Int)]()

      x.foreach(bmn => {
        bmn.schema.foreach(localSchema.add(_))
        bmn.types.map{case(name,localTypes) => {
          types.get(name) match {
            case Some(groupTypes) => localTypes.foreach(groupTypes.add(_))
            case None => types.put(name,localTypes)
          }
        }}
        mult += bmn.multiplicity
        subsets.append((bmn.types,bmn.multiplicity))
      })

      BiMaxNode(
        localSchema.toSet,
        types.toMap,
        mult,
        subsets
      )
    }).toSeq.to[mutable.Seq]

    return mutable.Seq(groups)
  }

}
