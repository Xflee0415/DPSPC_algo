package distribute

import smile.neighbor.{KDTree, Neighbor}

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.control.Breaks.{break, breakable}

//case class CachedPointAndDis(point: Point, distance: Double)
//case class LSHFunction(a: Array[Double], b: Double, w: Double)

class SPDataBlock(val pointsInPart: Array[Point]) extends Serializable {
  val size: Int = pointsInPart.length
  val dim: Int = pointsInPart.head.vector.length
//  private val projectLists: Array[Array[Double]] = Array.ofDim[Array[Double]](size)
  private val ranges: Array[List[Neighbor[Array[Double], Point]]] = Array.ofDim(size)
//  private var lshTables: Array[Array[LSHFunction]] = Array.empty
//  private var lshBuckets: Array[mutable.HashMap[String, mutable.HashSet[Point]]] = Array.empty
//  private val kNNs: Array[Array[Point]] = Array.ofDim(size)
  private var gammaList: Array[Double] = Array.ofDim(size)
//  private val cachedInfo: Array[Array[CachedPointAndDis]] = Array.ofDim(size)
  var centers: Array[Point] = Array.empty
  var clusters: Array[Cluster] = Array.empty

  for (i <- pointsInPart.indices) {
    pointsInPart(i).id = i
  }
//  println("point size: " + size)

  def localDPC(dc: Double, windowSize: Int, theta: Double): Unit = {
//    randomProject(numOfVectors, seed)
    rangeSearch(dc)
    fastComputeRho(dc)
    fastComputeDelta()
    computeGamma()
    findDensityPeaks(windowSize, theta)
    assignClusters()
  }

//  private def dotProduct(a: Array[Double], b: Array[Double]): Double = {
//    require(a.length == b.length, "Dimension of two vectors must be match!")
//    a.zip(b).map { case (x, y) => x * y }.sum
//  }

  private def rangeSearch(dc: Double): Unit = {
    println("Search range for all points......")
    val key = pointsInPart.map(_.vector)
    val value = pointsInPart
    val kdTree = new KDTree[Point](key, value)
    for (i <- pointsInPart.indices) {
      val queryPoint = pointsInPart(i)
      val tmpList = new util.ArrayList[Neighbor[Array[Double], Point]]()
      kdTree.range(queryPoint.vector, dc, tmpList)
      val neighborList = tmpList.asScala.toList
      ranges(i) = neighborList
    }
  }

//  private def lshPartition(numOfTable: Int, numOfFunc: Int, bucketWidth: Double): Unit = {
//    lshTables = (1 to numOfTable).map(_ => {
//      (1 to numOfFunc).map(_ => {
//        val gaussian = new Gaussian(0, 1)
//        LSHFunction(DenseVector.rand(dim, gaussian).toArray, math.random() * bucketWidth, bucketWidth)
//      }).toArray
//    }).toArray
//    lshBuckets = new Array[mutable.HashMap[String, mutable.HashSet[Point]]](numOfTable)
//    for (m <- 0 until numOfTable) {
//      lshBuckets(m) = new mutable.HashMap[String, mutable.HashSet[Point]]()
//      for (point <- pointsInPart) {
//        val hashValues = lshTables(m).map(func => {
//          ((dotProduct(func.a, point.vector) + func.b) / func.w).toInt
//        })
//        val key = hashValues.mkString(",")
//        if (!lshBuckets(m).contains(key)) {
//          lshBuckets(m)(key) = new mutable.HashSet[Point]()
//        }
//        lshBuckets(m)(key).add(point)
//      }
//    }
//    lshBuckets.foreach(bucket => {
//      bucket.keys.foreach(key => {
//        println("bucket size: " + bucket(key).size)
//      })
//    })
//  }

//  private def randomProject(numOfVectors: Int, seed: Long): Unit = {
//    val random = new Random(seed)
//    val projectVectors = (1 to numOfVectors).map(_ => {
//      val randomValues = (1 to dim).map(_ => random.nextDouble() - 0.5)
//      val norm = math.sqrt(randomValues.map(x => x * x).sum)
//      randomValues.map(_ / norm).toArray
//    }).toArray
//    for (index <- pointsInPart.indices) {
//      val point = pointsInPart(index)
//      val projectDisList = ListBuffer[Double]()
//      for (projectVector <- projectVectors) {
//        val projectDis = dotProduct(point.vector, projectVector)
//        projectDisList += projectDis
//      }
//      projectLists(index) = projectDisList.toArray
//    }
//  }

//  private def searchNeighbors(idx: Int, dc: Double): Array[Int] = {
//    val neighbors = ListBuffer[Int]()
//    var flag = false
//    for (i <- pointsInPart.indices) {
//      flag = true
//      breakable {
//        for (j <- projectLists(i).indices) {
//          if (projectLists(idx)(j) - dc >= projectLists(i)(j)
//            || projectLists(idx)(j) + dc <= projectLists(i)(j)) {
//            flag = false
//            break
//          }
//        }
//      }
//      if (flag) neighbors.append(i)
//    }
//    neighbors.toArray
//  }

//  private def computeKNNs(k: Int): Unit = {
//    var knnCount = 0
//    for (i <- pointsInPart.indices) {
//      val point = pointsInPart(i)
//      var unionSet = mutable.HashSet[Point]()
//      for (m <- lshBuckets.indices) {
//        val key = lshTables(m).map(func => {
//          ((dotProduct(func.a, point.vector) + func.b) / func.w).toInt
//        }).mkString(",")
//        val set = lshBuckets(m)(key)
//        unionSet = unionSet.union(set)
//      }
//      var knn = unionSet.toArray.sortBy(neighbor => neighbor.distance(point)).take(k)
//      if (knn.length < k) {
//        knnCount += 1
//        knn = pointsInPart.map(p => (p, p.distance(point)))
//          .sortBy(_._2).take(k + 1).map(_._1).filter(!_.equals(point))
//      }
//      kNNs(i) = knn
//      point.dmax = knn.last.distance(point)
//    }
//    println("knn<k count: " + knnCount)
//  }

  private def getDistance(i: Int, j: Int): Double = {
    require(i >= 0 && i < size && j >= 0 && j < size, "Index is not valid")
    if (i == j)
      0
    else
      pointsInPart(i).distance(pointsInPart(j))
  }

  private def fastComputeRho(dc: Double): Unit = {
    println("Computing rho for all points......")
    for (i <- pointsInPart.indices) {
      val point = pointsInPart(i)
      val neighbors = ranges(i)
      var expSum = 0.0
      for (neighbor <- neighbors) {
        val component = math.exp(-(neighbor.distance / dc) * (neighbor.distance / dc))
        expSum += component
      }
      point.rho = expSum
    }
  }

  private def fastComputeDelta(): Unit = {
    println("Computing delta for all points......")
    for (i <- pointsInPart.indices) {
      val point = pointsInPart(i)
      var minDis = Double.MaxValue
      var supID = -1
      for (neighbor <- ranges(i)) {
        if (neighbor.value.rho > point.rho) {
          val dis = neighbor.distance
          if (dis < minDis) {
            minDis = dis
            supID = neighbor.value.id
          }
        }
      }
      point.delta = minDis
      point.supID = supID
    }
    val remain = pointsInPart.filter(p => p.supID == -1)
    computeDelta(remain)
  }

  private def computeDelta(points: Array[Point]): Unit = {
    // sort the points by density (descending order)
    val sortedPoints = mutable.PriorityQueue(points: _*)(Ordering.by[Point, Double](_.rho))
    // get the point with maximum density
    val maxRhoPoint = sortedPoints.dequeue()
    // add the point with maximum density into higher density points list
    val maxRhoPoints = mutable.ArrayBuffer[Point]()
    val higherDensPoints = mutable.ArrayBuffer[Point]()
    maxRhoPoints += maxRhoPoint
    higherDensPoints += maxRhoPoint
    // process the points with same rho of maxRhoPoint
    var currentPoint = maxRhoPoint
    while (sortedPoints.nonEmpty && sortedPoints.head.rho == currentPoint.rho) {
      currentPoint = sortedPoints.dequeue()
      maxRhoPoints += currentPoint
      higherDensPoints += currentPoint
    }
    // compute the delta of each points
    var distance: Double = 0
    var maxDistance: Double = Double.MinValue
    while (sortedPoints.nonEmpty) {
      currentPoint = sortedPoints.dequeue()
      for (higherDensPoint <- higherDensPoints) {
        distance = getDistance(currentPoint.id, higherDensPoint.id)
        if (higherDensPoint.rho > currentPoint.rho && distance < currentPoint.delta) {
          currentPoint.delta = distance
          currentPoint.supID = higherDensPoint.id
        }
      }
      higherDensPoints += currentPoint
      if (currentPoint.delta > maxDistance) {
        maxDistance = currentPoint.delta
      }
    }
    // set the delta of the densest points as the maxDistance
    for (maxDensPoint <- maxRhoPoints) {
      maxDensPoint.delta = maxDistance
    }
  }

  private def computeGamma(): Unit = {
//    println("Computing gamma for all points......")
    var minGamma = Double.MaxValue
    var maxGamma = Double.MinValue
    for (i <- pointsInPart.indices) {
      gammaList(i) = pointsInPart(i).rho * pointsInPart(i).delta
      if (gammaList(i) < minGamma) {
        minGamma = gammaList(i)
      }
      if (gammaList(i) > maxGamma) {
        maxGamma = gammaList(i)
      }
    }
    gammaList = gammaList.map(gamma => (gamma - minGamma) / (maxGamma - minGamma))
  }

  private def findDensityPeaks(windowSize: Int, minVariance: Double): Unit = {
//    println("Finding density peaks by sliding window......")
    val sortedGamma = gammaList.sorted
    val start = (sortedGamma.length * 0.8).toInt
    val slice = sortedGamma.slice(start - windowSize, start)
    val window = ArrayBuffer[Double]()
    window.appendAll(slice)
    var variance = getWindowVariance(window)
    var inflexion = -1
    breakable {
      for (i <- start until sortedGamma.length) {
        window.remove(0)
        window.append(sortedGamma(i))
        variance = getWindowVariance(window)
        if (variance > minVariance) {
          inflexion = i
          break()
        }
      }
    }
    require(inflexion != -1, "Unable to find the inflection point, please adjust the parameters and try again!")
    val threshold = sortedGamma(inflexion)
//    println("Gamma threshold: " + threshold)
    val indices = gammaList.zipWithIndex.filter { case (value, _) => value >= threshold }.map(_._2)
    val densityPeaks = indices.map(pointsInPart(_))
    densityPeaks.foreach(center => center.isDP = true)
//    println("Find " + densityPeaks.length + " density peaks!")
  }

  private def getWindowVariance(window: ArrayBuffer[Double]): Double = {
    val mean = window.sum / window.size
    val variance = window.map(x => math.pow(x - mean, 2)).sum / window.size
    variance
  }

  private def assignClusters(): Unit = {
    centers = pointsInPart.filter(p => p.isDP)
    var cluID = 1
    for (center <- centers) {
      center.cluID = cluID
      cluID += 1
    }
    for (point <- pointsInPart.sortBy(_.rho)(Ordering[Double].reverse)) {
      if (point.cluID == 0) {
        val superPoint = pointsInPart(point.supID)
        point.cluID = superPoint.cluID
      }
    }
    val clusterMap = mutable.Map[Int, ArrayBuffer[Long]]()
    for (point <- pointsInPart) {
      val label = point.cluID
      if (!clusterMap.keySet.contains(label))
        clusterMap(label) = ArrayBuffer()
      clusterMap(label).append(point.uid)
    }
    clusters = new Array[Cluster](centers.length)
    for (c <- centers.indices) {
      val label = centers(c).cluID
      clusters(c) = Cluster(centers(c), clusterMap(label).toArray, label)
    }
  }

}
