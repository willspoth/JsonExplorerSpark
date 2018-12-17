package NMF

import breeze.linalg._
import breeze.numerics._
import breeze.optimize.linear.NNLS
import breeze.stats._
import smile.read

import scala.collection.mutable
import scala.util.control.Breaks._


object DataUtils{

  def getData(datapath: String, multpath: String): (DenseMatrix[Double], DenseVector[Double]) = {
    val data = read.libsvm(datapath)
    val (dataMatrix_raw, l) = data.unzipInt
    val dataMatrix_smile = DenseMatrix(dataMatrix_raw:_*)
    //load multiplicity
    val multiplicities_raw = scala.io.Source.fromFile(multpath).getLines.toArray.flatMap(_.split("\n")).map(_.toDouble)
    val multiplicities_smile = breeze.linalg.DenseVector(multiplicities_raw)
    val numTuples = dataMatrix_smile.rows
    val numFeatures = dataMatrix_smile.cols
    //adjust by multiplicities
    println(numTuples + " X " + numFeatures + " data matrix loaded with multiplicity sum to " + sum(multiplicities_smile))
    (dataMatrix_smile, multiplicities_smile)
  }

}

class NMFBiCluster_Scala(inputMatrix:DenseMatrix[Double],multVector:DenseVector[Double], inputfiles:Option[(String,String)] = None) {

  var dataMatrix_smile = inputMatrix
  var multiplicities_smile = multVector

  inputfiles match {
    case Some((fvfile,multfile)) =>
      val (dataMatrix, multiplicities) = DataUtils.getData(fvfile, multfile)
      //sanity check if the in-memory matrix aligns with the matrix read from disk
      val fvmismatch = sum(dataMatrix_smile :!= dataMatrix)
      val multmismatch = sum(multiplicities_smile :!= multiplicities)
      if (fvmismatch == true || multmismatch == true ){
        throw new Exception("in-memory matrix provided and the one read one disk does not match, please check")
      }
    case None =>
    //do nothing
  }
  //load data
  val (basisVectors, projectedMatrix, residual) = NMFBiClustering()
  val ata = basisVectors.t*basisVectors
  var featureLabels = Array.range(-1,0)
  var dataLabels = Array.range(-1,0)
  var fptree = new FPTree(Array[Int](0))

  /**
    * Major function for NMF Bi-Clustering
    *
    * @param X    input dataMatrix, columns should be data tuples for performance benefits
    * @param k    the number of basis vecors needed
    * @param mult multiplicities for data tuples
    * @return
    */
  def NMF(X: DenseMatrix[Double], k: Int, mult: DenseVector[Double]): (DenseMatrix[Double], DenseMatrix[Double], Double) = {
    val r = X.rows
    val c = X.cols
    val maxIter = 200
    val residualCoeff = 1e-4
    val tofCoeff = 1e-4

    val weights = sqrt(mult)

    val refNorm = sqrt(sum(weights *:* weights) * r)
    val residual = residualCoeff * refNorm
    val tof = tofCoeff * refNorm

    implicit def bool2double(b: Boolean): Double = if (b) 1.0 else 0.0

    val eps = 1e-32
    val oriMatrix = X(*, ::) * weights
    val Matrix = oriMatrix + eps

    if (k > 1) {
      val x = Array.ofDim[Double](c, r)
      for (i <- 0 until c) {
        x(i) = oriMatrix(::, i).toArray
      }
      val clusters = smile.clustering.kmeans(x, k, runs = 3)
      val centroids = clusters.centroids()
      val inx = DenseVector(clusters.getClusterLabel)
      var A = DenseMatrix(centroids: _*).t+eps
      val YY = (inx * DenseMatrix.ones[Int](1, k) - DenseMatrix.ones[Int](c, 1) * accumulate(DenseMatrix.ones[Int](1, k), Axis._0)) :== 0
      var Y = YY.map(x => bool2double(x))
      Y = Y.t + 0.2
      var MatrixfitPrevious = Matrix.map(x => Double.MaxValue)
      var finalResidual = -1.0;


      breakable {
        for (iter <- 1 to maxIter) {
          val temp = Matrix * Y.t
          A = A *:* ( temp /:/ (A * A.t * temp))
          Y = Y *:* ((A.t * Matrix) /:/ (A.t * A * Y))
          if (iter % 50 == 0 || iter == maxIter) {
            //println("Iterating " + iter + " th")
            val MatrixfitThis = A * Y
            val fitResM = MatrixfitPrevious - MatrixfitThis
            val fitRes = sqrt(sum(fitResM *:* fitResM))
            MatrixfitPrevious = MatrixfitThis
            val curResM = Matrix - MatrixfitThis
            val curRes = sqrt(sum(curResM *:* curResM))
            if (tof >= fitRes || residual >= curRes || iter == maxIter) {
              println("NMF finished at %d-th iteration with residual %.3f".format(iter, curRes))
              finalResidual = curRes
              break
            }
          }
        }
      }
      //remove multiplicity info in Y
      Y = Y(*,::) / weights
      //      //normalize Y and A
      //      val Norms = norm(A,Axis._0).t
      //      A = A(*,::) / Norms
      //      Y = Y(::,*) * Norms
      val Norms = max(A,Axis._0).t
      Y = Y(::,*) * Norms
      A = A(*,::) / Norms

      (A, Y, finalResidual)
    }
    else {
      val centroids = mean(Matrix, Axis._1)
      var A = DenseMatrix.zeros[Double](Matrix.rows, 1);
      A(::, 0) := centroids+eps
      val YY = (DenseMatrix.zeros[Int](Matrix.cols, k) - DenseMatrix.ones[Int](c, 1) * accumulate(DenseMatrix.ones[Int](1, k), Axis._0)) :== 0
      var Y = YY.map(x => bool2double(x))
      Y = Y.t + 0.2
      var MatrixfitPrevious = Matrix.map(x => Double.MaxValue)
      var finalResidual = -1.0;

      breakable {
        for (iter <- 1 to maxIter) {
          val temp = Matrix * Y.t
          A = A *:* (temp /:/ (A * A.t * temp))
          Y = Y *:* ((A.t * Matrix) /:/ (A.t * A * Y))
          if (iter % 50 == 0 || iter == maxIter) {
            //println("Iterating " + iter + " th")
            val MatrixfitThis = A * Y
            val fitResM = MatrixfitPrevious - MatrixfitThis
            val fitRes = sqrt(sum(fitResM *:* fitResM))
            MatrixfitPrevious = MatrixfitThis
            val curResM = Matrix - MatrixfitThis
            val curRes = sqrt(sum(curResM *:* curResM))
            if (tof >= fitRes || residual >= curRes || iter == maxIter) {
              println("NMF finished at %d-th iteration with residual %.3f".format(iter, curRes))
              finalResidual = curRes
              break
            }
          }
        }
      }
      //remove multiplicity info in Y
      Y = Y(*,::) / weights
      //      //normalize Y and A
      //      val Norms = norm(A,Axis._0).t
      //      A = A(*,::) / Norms
      //      Y = Y(::,*) * Norms
      val Norms = max(A,Axis._0).t
      Y = Y(::,*) * Norms
      A = A(*,::) / Norms

      (A, Y, finalResidual)
    }
  }

  def NMFBiClustering(): (DenseMatrix[Double], DenseMatrix[Double], Double) = {
    //determine k for bi-clustering
    val direction = 0 //0 means treating rows as data tuples and 1 means treating cols as data tuples for clustering
    if (direction ==0)
      println("Determining proper k based on clustering data tuples.")
    else
      println("Determining proper k based on clustering features.")

    var k = determineK(direction)
    println("k evaluated to be " + k)
    println("Begin NMF")
    // check if k==1 is possible
    val thres = 0.1;
    val X = dataMatrix_smile.t;

    if (k == 2) {
      val (basisVectors2, projectedMatrix2, residual2) = NMF(X, 2, multiplicities_smile)
      println("checking if k==1 is possible")
      val (basisVectors1, projectedMatrix1, residual1) = NMF(X, 1, multiplicities_smile)
      if ((residual1 - residual2) / residual1 < thres) {
        println("k adjusted to be 1. ")
        (basisVectors1, projectedMatrix1, residual1)
      }
      else {
        (basisVectors2, projectedMatrix2, residual2)
      }
    }
    else {
      val (basisVectors, projectedMatrix, residual) = NMF(X, k, multiplicities_smile)
      (basisVectors, projectedMatrix, residual)
    }
  }

  /**
    * Output: a vector of length K, each i-th (i=1,...,K) value is within [0,1] indicating the membership confidence of feature group i
    *
    * @param featurevector
    * @return
    */
  def getFeatureGroupMembershipConfidence(featurevector: Array[Double]): DenseVector[Double] = {
    val vec = DenseVector(featurevector)
    val nnls = new NNLS()
    val atb = basisVectors.t*vec
    nnls.minimize(ata, atb)
  }

  /**
    * get the indexes of the reordered features
    * @return
    */
  def getFeatureOrder(): (Array[Int]) = {
    if (this.featureLabels(0) == -1)
      this.featureLabels = hierarchicalClusteringSort(this.basisVectors)

    this.featureLabels
  }

  def getDataOrder(): (Array[Int]) = {
    if (this.dataLabels(0) == -1) {
      val featureGroupLabels = hierarchicalClusteringSort(this.basisVectors.t)
      //re-order ProjectedMatrix
      val newProjectedMatrix = DenseMatrix.zeros[Double](this.projectedMatrix.cols,this.projectedMatrix.rows)
      for (i <- 0 until this.projectedMatrix.rows){
        newProjectedMatrix(::,i) := this.projectedMatrix(featureGroupLabels(i),::).t
      }
      this.dataLabels = multipleFeatureSort(newProjectedMatrix)
    }
    this.dataLabels
  }

  def hierarchicalClusteringSort(InputMatrix:DenseMatrix[Double]): Array[Int] = {
    if (InputMatrix.rows>1) {
      val x = Array.ofDim[Double](InputMatrix.rows, InputMatrix.cols)
      for (i <- 0 until InputMatrix.rows) {
        x(i) = InputMatrix(i, ::).t.toArray
      }
      val clusters = smile.clustering.hclust(smile.util.pdist(x), "complete")
      val labels = new Array[Int](InputMatrix.rows)
      //interpret labels by merge dendrogram
      val Tree = clusters.getTree
      val stepMap: mutable.HashMap[Int, Array[Int]] = new mutable.HashMap[Int, Array[Int]]()
      for (step <- 0 until Tree.length) {
        val children = Tree(step)
        var ArrayLeft = new Array[Int](1)
        if (children(0) < InputMatrix.rows)
          ArrayLeft(0) = children(0)
        else
          ArrayLeft = stepMap(children(0) - InputMatrix.rows)

        var ArrayRight = new Array[Int](1)
        if (children(1) < InputMatrix.rows)
          ArrayRight(0) = children(1)
        else
          ArrayRight = stepMap(children(1) - InputMatrix.rows)

        val ArrayMerge = ArrayLeft ++ ArrayRight
        stepMap += (step -> ArrayMerge)
      }
      stepMap(InputMatrix.rows - 2)
    }
    else
      new Array[Int](1)
  }

  /**
    *
    * @param InputMatrix assume rows are data tuples and column values indicate existence of certain features, sort
    *  rows by column values
    * @return indexes of data tuples after sort
    */
  def multipleFeatureSort(InputMatrix:DenseMatrix[Double]): Array[Int] ={
    val Indexes = Array.ofDim[Int](InputMatrix.rows,InputMatrix.cols)
    for (i <- 0 until InputMatrix.rows){
      val vec = InputMatrix(i,::).t
      val ind = argsort(vec).reverse
      Indexes(i) = ind.toArray
    }
    val I = DenseMatrix(Indexes:_*)
    val (labels,largestLabelIDAssigned) = multiLayerSort(I,0)
    labels.toArray
  }

  def multiLayerSort(I : DenseMatrix[Int],labelStartValue: Int): ( DenseVector[Int] , Int)={
    val numLayers = I.cols
    val numDataPoints= I.rows
    val topLayerLabels=I(::,0)
    val topLayerUniqueLabels=unique(topLayerLabels)
    //default assigning all items to be of starting label IDs
    val labels=DenseVector.fill[Int](numDataPoints){labelStartValue}

    // if only one layer
    if (numLayers == 1){
      val largestLabelIDAssigned=labelStartValue+topLayerUniqueLabels.length-1
      val uniqueLabelsForAssignment=labelStartValue to largestLabelIDAssigned
      for (i <- 0 until topLayerUniqueLabels.length) {
        val indexes = (topLayerLabels :== topLayerUniqueLabels(i)).activeKeysIterator.toIndexedSeq
        labels(indexes) := uniqueLabelsForAssignment(i)
      }
      (labels,largestLabelIDAssigned)
    }
    else {
      //if more than one layer
      if (topLayerUniqueLabels.length > 1){
        var largestLabelIDAssigned = labelStartValue-1
        for (i <- 0 until topLayerUniqueLabels.length){
          val labelID=topLayerUniqueLabels(i)
          val indexes= (topLayerLabels :== labelID).activeKeysIterator.toIndexedSeq
          val labelSpecific_I = I(indexes, 1 until numLayers).toDenseMatrix
          val (labels_partitionSpecific,lID)=multiLayerSort(labelSpecific_I,largestLabelIDAssigned+1)
          largestLabelIDAssigned=lID
          labels(indexes) := labels_partitionSpecific
        }
        (labels,largestLabelIDAssigned)
      }
      else {
        val largestLabelIDAssigned = labelStartValue
        (labels,largestLabelIDAssigned)
      }
    }
  }

  def determineK(direction: Int): Int = {
    //PCA normalize and adjust by multiplicities
    var scores = PCANormalize(dataMatrix_smile, direction, multiplicities_smile)
    //translate back to smile data frame
    val x = Array.ofDim[Double](scores.rows, scores.cols)
    for (i <- 0 until scores.rows) {
      x(i) = scores(i, ::).t.toArray
    }

    val cap = 20
    var k = -1
    var evals = new Array[Double](cap - 1)
    val round =5

    for (i <- 2 until cap + 1) {
      val clusters = smile.clustering.kmeans(x, i, runs = round)
      val labels = clusters.getClusterLabel
      val eval = evaluateCluster(labels, scores, i, clusters.getClusterSize, clusters.centroids(), "daviesbouldin")
      evals(i - 2) = eval
    }

    val ind = argmax(evals)
    k = ind + 2
    k
  }

  def evaluateCluster(labels: Array[Int], x: DenseMatrix[Double], k: Int, sizes: Array[Int], centroids: Array[Array[Double]], metricName: String): Double = {
    if (metricName.toLowerCase == "silhouette") {
      val numTuples = x.rows
      val BigM = DenseMatrix.zeros[Double](numTuples, k);
      val centroidM = DenseMatrix(centroids: _*)

      for (i <- 0 until k) {
        val diffM = x(*, ::) - centroidM(i, ::).t
        val square = diffM *:* diffM
        val norms = sqrt(sum(square, Axis._1))
        BigM(::, i) := norms
      }
      val a_iVec = DenseVector.zeros[Double](numTuples)
      for ((label, i) <- labels.zipWithIndex) {
        a_iVec(i) = BigM(i, label)
        BigM(i, label) = Double.MaxValue
      }
      val b_iVec = min(BigM, Axis._1)

      //compute sihouette
      val sihouettes = (b_iVec - a_iVec) /:/ max(a_iVec, b_iVec)
      mean(sihouettes)
    }
    else if (metricName.toLowerCase == "daviesbouldin") {
      val myMap:mutable.HashMap[Int, DenseMatrix[Double]] = new mutable.HashMap[Int, DenseMatrix[Double]]()
      val countMap:mutable.HashMap[Int, Int] =new mutable.HashMap[Int, Int]()
      //initialize hashmap
      for (i <- 0 until k){
        myMap += (i -> DenseMatrix.zeros[Double](sizes(i),x.cols))
        countMap += (i -> 0)
      }

      for ((label, i) <- labels.zipWithIndex) {
        val M=myMap(label)
        val c=countMap(label)
        M(c,::) := x(i,::)
        val cc = c+1
        countMap += (label -> cc)
      }
      val S = DenseVector.zeros[Double](k)
      val centroidM = DenseMatrix(centroids:_*)
      for (i <- 0 until k){
        val centroid = centroidM(i,::).t
        val M = myMap(i)
        val diffM = M(*,::) - centroid
        val S_i = mean(sqrt(sum(diffM *:* diffM,Axis._1)))
        S(i) = S_i
      }

      val R = DenseMatrix.zeros[Double](k,k)
      for (i <- 0 until k){
        for (j<- i+1 until k){
          val diff = centroidM(i,::)-centroidM(j,::)
          val norm = sqrt(sum(diff *:* diff))
          val ratio = (S(i)+S(j))/norm
          R(i,j)=ratio
          R(j,i)=ratio
        }
      }
      1/mean(max(R,Axis._1))
    }
    else {
      println("invalid clustering evaluation metric.")
      nan
    }
  }

  /**
    * normalize and do PCA to reduce dimensionality
    *
    * @param DMatrix rows represents data tuples
    * @param d       dimension, whether rows (dimension=0) or cols (dimension=1) are treated as data tuples
    *                @
    */
  def PCANormalize(DMatrix: breeze.linalg.DenseMatrix[Double], d: Int, multiplicities: breeze.linalg.DenseVector[Double]): breeze.linalg.DenseMatrix[Double] = {
    println("Begin normalization and PCA reduction.")
    val threshold = 0.99
    val eps = 2.2204e-16
    val weights = sqrt(multiplicities)

    if (d == 0) {
      var normalizedDMatrix = DMatrix(*,::) - mean(DMatrix,Axis._0).t
      val stds = stddev(normalizedDMatrix,Axis._0).t + eps
      normalizedDMatrix = normalizedDMatrix(*,::) / stds

      //adjust by multiplicities
      val multiAdjustedDMatrix = normalizedDMatrix(::, *) * weights

      //do PCA
      val Xsquare = multiAdjustedDMatrix.t * multiAdjustedDMatrix
      val svd.SVD(u, s, vt) = svd(Xsquare)
      //val ss = sqrt(s)
      val ss = s
      val cumsums = accumulate(ss)
      val totalsum = sum(ss)
      var numPCs = -1
      breakable {
        for (i <- 0 until cumsums.length) {
          if (cumsums(i) >= totalsum * threshold) {
            numPCs = i + 1
            break
          }
        }
      }
      val basisVectors = u(::, 0 until numPCs)
      val scores = multiAdjustedDMatrix * basisVectors
      println("PCA finished. " + multiAdjustedDMatrix.cols + " dimensions has been reduced to " + numPCs)
      scores
    }
    else if (d == 1) {
      var normalizedDMatrix= DMatrix(::,*) - mean(DMatrix,Axis._1)
      val stds = stddev(normalizedDMatrix,Axis._1)+eps
      normalizedDMatrix = normalizedDMatrix(::,*) / stds
      //adjust by multiplicities
      val multiAdjustedDMatrix = normalizedDMatrix(::, *) * weights
      //do PCA
      val svd.SVD(u, s, vt) = svd.reduced(multiAdjustedDMatrix)
      val ss = s *:* s
      val cumsums = accumulate(ss)
      val totalsum = sum(ss)
      var numPCs = -1
      breakable {
        for (i <- 0 until cumsums.length) {
          if (cumsums(i) >= totalsum * threshold) {
            numPCs = i + 1
            break
          }
        }
      }
      val basisVectors = u(::, 0 until numPCs)
      val scores = multiAdjustedDMatrix.t * basisVectors
      println("PCA finished. " + multiAdjustedDMatrix.rows + " dimensions has been reduced to " + numPCs)
      scores
    }
    else {
      println("error, normalization direction must be either 0(rowwise) or 1(columnwise).")
      null
    }
  }


  val threshold = 0.1
  /**
    * check if a given feature vector matches the patterns captured
    * @return
    */
  def ifMatch(featurevector: Array[Double]): Boolean = {
    if (this.fptree.isEmpty()) {
      val reconstructed = this.basisVectors * this.projectedMatrix
      this.fptree = this.buildFPTree(reconstructed,this.getFeatureOrder(),this.threshold)
    }
    this.fptree.ifMatch(featurevector)
  }

  /**
    * treat the NMF result as a regular expression and check how many expressions it covers
    * @return
    */
  def getCoverage(): Int ={
    if (this.fptree.isEmpty()) {
      val reconstructed = this.basisVectors * this.projectedMatrix
      this.fptree = this.buildFPTree(reconstructed,this.getFeatureOrder(),this.threshold)
    }
    this.fptree.getNumOfLeaves()
  }

  /**
    * build an FPTree from a matrix of double values
    * values below a threshold = 0 and values above 1- threshold = 1
    * and values within 0 and 1 means optional
    * @param input
    * @param featureOrder
    * @return
    */
  def buildFPTree(input:DenseMatrix[Double],featureOrder: Array[Int], threshold: Double): FPTree ={
    val mytree = new FPTree(featureOrder)
    mytree.consumeAll(input,threshold)
    mytree
  }
}

class FPTree (fOrder: Array[Int]) {

  /**
    * ID = -1 means rootNode
    * @param featureID
    */
  class FPNode (featureID : Int) {
    var occurCount = 0
    val ID = featureID
  }

  var root = new FPNode(-1)
  val featureOrder = fOrder

  def isEmpty(): Boolean ={
    true
  }

  def consume(): Unit = {

  }

  def consumeAll(input:DenseMatrix[Double],threshold : Double): Unit ={
    val reorderedInput = DenseMatrix.zeros[Double](input.rows,input.cols)
    for (i <- 0 until this.featureOrder.length) {
      reorderedInput(::,i) := input(::,this.featureOrder(i))
    }
    //todo
  }

  def branch (): Unit ={

  }

  def ifMatch(featurevector: Array[Double]): Boolean ={
    true
  }

  def getNumOfLeaves(): Int ={
    0
  }


}
