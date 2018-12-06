import NMF.NMFBiCluster_Scala
import breeze.linalg.norm

object NMFRun {
  def main(args: Array[String]) = {
    //path to data directory
    val rootDirectory = "C:\\Users\\Will\\Documents\\GitHub\\JsonGuide\\data\\clusters\\yelpTest\\"//"C:\\Users\\Will\\Documents\\GitHub\\JsonExplorer\\"
    val path = rootDirectory
    val multName = "multoutput.txt"
    //feature vector file name
    val fname = "fvoutput.txt"

    val c = new NMFBiCluster_Scala(path + fname, path + multName)

    //sanity check whether function getFeatureGroupMembershipConfidence works as expected
    //Output of getFeatureGroupMembershipConfidence() is a vector of length K, each i-th (i=1,...,K) value is within [0,1] indicating the membership confidence of feature group i
    val reconstructed = c.basisVectors * c.projectedMatrix
    for (i <- 0 until reconstructed.cols) {
      val vec = reconstructed(::, i)
      //get feature group confidences
      val vec1 = c.getFeatureGroupMembershipConfidence(vec.toArray)
      //get feature vectors of original data matrix that have been projected onto basis vectors
      val vec2 = c.projectedMatrix(::, i)
      //group confidences should align with projected rows of the matrix
      val diff = norm(vec1 - vec2)
      if (diff > 1E-13)
        println(diff)
    }
    //check if functions that gets the re-ordering of features and data tuples work without error
    val indACluster = c.getFeatureOrder()
    val indYCluster = c.getDataOrder()
  }
}
