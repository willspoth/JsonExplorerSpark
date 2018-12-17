import NMF._

import breeze.linalg.{norm, sum}
import breeze.numerics.abs

object NMFRun extends App{
  //path to data directory
  val path="C:\\Users\\Will\\Documents\\GitHub\\JsonExplorerSpark\\FeatureVectors\\"
  val multName="root.mults"
  //feature vector file name
  val fname="root.features"

  val (dataMatrix_smile, multiplicities_smile) = DataUtils.getData(path+fname, path+multName)
  val c = new NMFBiCluster_Scala(dataMatrix_smile, multiplicities_smile)
  //sanity check whether function getFeatureGroupMembershipConfidence works as expected
  //Output of getFeatureGroupMembershipConfidence() is a vector of length K, each i-th (i=1,...,K) value is within [0,1] indicating the membership confidence of feature group i
  //val reconstructed = c.basisVectors * c.projectedMatrix
  val orginal = c.dataMatrix_smile.t
  for (i <- 0 until orginal.cols){
    val vec = orginal(::,i)
    val vecArray = vec.toArray
    //get feature group confidences
    val vec1 = c.getFeatureGroupMembershipConfidence(vecArray)
    //get feature vectors of original data matrix that have been projected onto basis vectors
    // val vec2 = c.projectedMatrix(::,i)
    //group confidences should align with projected rows of the matrix
    //val diff = sum(abs(vec1 - vec2))
    val reconstucted = c.basisVectors*vec1
    val diffvec = vec-reconstucted
    val thresh = 0.2
    val aha = diffvec >:> thresh
    val wuhu = diffvec <:< -thresh
    implicit def bool2double(b: Boolean): Double = if (b) 1.0 else 0.0
    val ahaha = aha.map(x => bool2double(x))
    val wuhuhu = wuhu.map(x => bool2double(x))
    //println (sum(ahaha))
    //println (sum (wuhuhu))
  }
  c.getCoverage()
}
