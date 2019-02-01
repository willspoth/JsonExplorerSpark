package NMF

import Explorer.{JsonExtractionSchema, Types}
import Explorer.Types.SchemaName
import breeze.linalg.{*, DenseMatrix, DenseVector}

object RunNMF {

  // coverage, column order, row order
  def runNMF(schema: JsonExtractionSchema, fvs: DenseMatrix[Double], mults: DenseVector[Double]): (Int,Array[Int],Array[Int]) = {
    //path to data directory
    if(fvs.data.isEmpty)
      return (0,null,null)

    val orig = fvs(*, ::).map( dv => dv.toArray).toArray

    val c = new NMFBiCluster_Scala(fvs, mults)
    //sanity check whether function getFeatureGroupMembershipConfidence works as expected
    //Output of getFeatureGroupMembershipConfidence() is a vector of length K, each i-th (i=1,...,K) value is within [0,1] indicating the membership confidence of feature group i
    //val reconstructed = c.basisVectors * c.projectedMatrix
    val orginal = c.dataMatrix_smile.t
    for (i <- 0 until orginal.cols){
      val vec = orginal(::,i)
      val vecArray = vec.toArray
      //get feature group confidences
      val vec1 = c.getFeatureGroupMembershipConfidence(vecArray)

      val reconstucted = c.basisVectors*vec1
      val diffvec = vec-reconstucted
      val thresh = 0.2
      val aha = diffvec >:> thresh
      val wuhu = diffvec <:< -thresh
      implicit def bool2double(b: Boolean): Double = if (b) 1.0 else 0.0
      val ahaha = aha.map(x => bool2double(x))
      val wuhuhu = wuhu.map(x => bool2double(x))

    }
    /*
        val numClusters = 2
        val numIterations = 20
        val clusters = smile.clustering.KMeans.lloyd(orig,numClusters,numIterations)
        val clusterLabels = clusters.getClusterLabel
        val c1 = orig.zip(clusterLabels).sortBy(_._2).map(_._1)

        val d = fvs(*, ::).map( dv => dv.toArray.zip(c.getFeatureOrder()).sortBy(_._2).map(_._1)).toArray.zip(c.getDataOrder()).sortBy(_._2).map(_._1)

        val frame: JFrame = new JFrame(Types.nameToString(name))
        frame.setPreferredSize(new Dimension(3000, 1500))
        frame.setLayout(new GridLayout(1,2))

        val p1 = smile.plot.heatmap(orig)
        val p3 = smile.plot.heatmap(c1)
        val p2 = smile.plot.heatmap(d)

        p1.close
        p2.close
        p3.close

        p1.canvas.setTitle("Original")
        p3.canvas.setTitle("KMeans k=" + numClusters.toString)
        p2.canvas.setTitle("Reshaped")

        frame.getContentPane.add(p1.canvas)
        frame.getContentPane.add(p3.canvas)
        frame.getContentPane.add(p2.canvas)

        p1.canvas.reset()
        p2.canvas.reset()
        p3.canvas.reset()

        frame.pack()
        frame.setVisible(true)

    */

    //(c.getCoverage(),c.getFeatureOrder(),c.getDataOrder())


    schema.parent // this is the schema name, can use for debugging
    val lookup = schema.attributeLookup.map(x => (x._2,x._1))
    Types.nameToString(lookup.get(0).get) // name of the 0th element in the schema to string

    (0,c.getFeatureOrder(),c.getDataOrder())
  }
}
