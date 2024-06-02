package distribute

import scala.collection.mutable
import scala.util.Random

class ClustersCombiner(clusters: Array[Cluster], numOfParts: Int) {
  private val n: Int = clusters.length
  private val distMap: Map[String, Double] = calDistMap().toMap
  private var knnGraph: Graph = _
  private var knnSet: Array[Set[Int]] = _
  private val targetCN: Int = clusters.length / numOfParts
  private val range: Int = (targetCN * 0.1).ceil.toInt

  private def calDistMap(): mutable.Map[String, Double] = {
    val distanceMap = mutable.Map[String, Double]()
    for (i <- 0 until n - 1) {
      for (j <- i + 1 until n) {
        val distance = clusters(i).centroid.distance(clusters(j).centroid)
        distanceMap(s"$i,$j") = distance
      }
    }
    distanceMap
  }

  private def getDist(i: Int, j: Int): Double = {
    if (i < j) {
      distMap(s"$i,$j")
    } else if (i > j) {
      distMap(s"$j,$i")
    } else {
      0
    }
  }

  private def calKNN(K: Int): Unit = {
    val knnMat = Array.ofDim[Int](n, K)
    for (i <- 0 until n) {
      val distances = Array.fill(n)(Double.PositiveInfinity)
      for (j <- 0 until n) {
        if (i != j) distances(j) = getDist(i, j)
      }
      for (j <- 0 until K) {
        val min_index = distances.indexOf(distances.min)
        knnMat(i)(j) = min_index
        distances(min_index) = Double.PositiveInfinity
      }
    }
    knnSet = knnMat.map(_.toSet)
  }

  def mergeClusters(): Map[String, Int] = {
    require(numOfParts > 0, "Must be at least one partition!")
    println(s"Begin to merge the clusters:")
    var low = 0
    var high = numOfParts
    var testK = 0
    var clusterNum = 0
    while (low < high) {
      testK = (low + high) / 2
      calKNN(testK)
      clusterNum = merge()
      if (clusterNum >= targetCN - range && clusterNum <= targetCN + range) {
        println(s"Find K: $testK, resulting in $clusterNum clusters")
        return resultMap()
      } else if (clusterNum < targetCN) {
        high = testK - 1
      } else {
        low = testK + 1
      }
    }
    if (low == high) {
      calKNN(low)
      val testNum = testMerge()
      if (math.abs(testNum - targetCN) < math.abs(clusterNum - targetCN)) {
        testK = low
        clusterNum = merge()
      }
    }
    println(s"Find K: $testK, resulting in $clusterNum clusters")
    resultMap()
  }

  private def merge(): Int = {
    knnGraph = new Graph(n)
    for (cluster <- clusters) cluster.visited = false
    for (i <- 0 until n - 1) {
      for (j <- i + 1 until n) {
        if (knnSet(i).contains(j) && knnSet(j).contains(i))
          knnGraph.addEdge(i, j)
      }
    }
    var label = 1
    for (i <- 0 until n) {
      if (!clusters(i).visited) {
        dfs_assign(i, label)
        label += 1
      }
    }
    label - 1
  }

  private def testMerge(): Int = {
    knnGraph = new Graph(n)
    for (cluster <- clusters) cluster.visited = false
    for (i <- 0 until n - 1) {
      for (j <- i + 1 until n) {
        if (knnSet(i).contains(j) && knnSet(j).contains(i))
          knnGraph.addEdge(i, j)
      }
    }
    var label = 1
    for (i <- 0 until n) {
      if (!clusters(i).visited) {
        dfs(i)
        label += 1
      }
    }
    label - 1
  }

  private def resultMap(): Map[String, Int] = {
    (0 until n).map { i =>
      clusters(i).uuid -> clusters(i).label
    }.toMap
  }

  def takeRepresentatives(numOfRep: Int): Map[Int, Array[Long]] = {
    val sampleReps = mutable.Map[Int, Array[Long]]()
    val countSet = mutable.Map[Int, Int]()
    val random = new Random
    for (cluster <- clusters) {
      val label = cluster.label
      if (!sampleReps.keySet.contains(label)) {
        sampleReps(label) = Array.ofDim[Long](numOfRep)
        countSet(label) = 0
      }
      for (pid <- cluster.points) {
        val count = countSet(label)
        if (count < numOfRep) {
          sampleReps(label)(count) = pid
        } else {
          val r = random.nextInt(count + 1)
          if (r < numOfRep) {
            sampleReps(label)(r) = pid
          }
        }
        countSet(label) = count + 1
      }
    }
    sampleReps.toMap
  }

  private def dfs_assign(u: Int, label: Int): Unit = {
    clusters(u).label = label
    clusters(u).visited = true
    for (v <- knnGraph.adjacencyList(u)) {
      if (!clusters(v).visited)
        dfs_assign(v, label)
    }
  }

  private def dfs(u: Int): Unit = {
    clusters(u).visited = true
    for (v <- knnGraph.adjacencyList(u)) {
      if (!clusters(v).visited)
        dfs(v)
    }
  }

  private class Graph(n: Int) {
    // Use a Map to represent the adjacency list,
    // where the keys are vertices and the values are lists of vertices adjacent to the key vertex.
    var adjacencyList: Map[Int, List[Int]] = Map()
    for (i <- 0 until n) addVertex(i)


    // add vertices
    private def addVertex(vertex: Int): Unit = {
      if (!adjacencyList.contains(vertex)) {
        adjacencyList += (vertex -> List())
      }
    }

    // add edges
    def addEdge(vertex1: Int, vertex2: Int): Unit = {
      // ensure vertices exist
      if (adjacencyList.contains(vertex1) && adjacencyList.contains(vertex2)) {
        // undirected graph
        adjacencyList += (vertex1 -> (vertex2 :: adjacencyList(vertex1)))
        adjacencyList += (vertex2 -> (vertex1 :: adjacencyList(vertex2)))
      }
    }
  }
}
