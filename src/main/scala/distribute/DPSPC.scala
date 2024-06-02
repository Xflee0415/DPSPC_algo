package distribute

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

class DPSPC(val inPath: String,
            val outPath: String,
            val dim: Int,
            val numPart: Int,
            val sampleSize: Int = 3,
            val pct: Double = 0.02,
            val windowSize: Int = 20,
            val theta: Double = 2e-5,
            val numOfRep: Int = 5) extends Serializable {

  def runDPSPC(): Unit = {
    // Set the log level to WARN to disable INFO level logs
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Configure the Spark environment
    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("DPSPC-Cluster")
      .getOrCreate()
    val sc = spark.sparkContext
    // Generate the corresponding SPDB file path based on the input data path
    // If the SPDB file already exists, there is no need to regenerate it
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val spdbPath = inPath.stripSuffix(".txt") + s"-rsp-$numPart"
    if (!fs.exists(new Path(spdbPath))) {
      SPDBGenerator.generateSPDB(sc, inPath, spdbPath, numPart)
    }
    // Read the SPDB
    val reader = new SPDBReader(sc, spdbPath)
    reader.sampleSPDBs(sampleSize)

    // generate a random projection matrix
//    val et = 0.3
//    val totalNum = reader.countRecords()
//    val targetDim = (math.log(totalNum) / (math.pow(et, 2) - math.pow(et, 3))).toInt
//    var projMatBC = sc.broadcast[DenseMatrix[Double]](DenseMatrix.zeros(1, 1))
//    if (targetDim * 5 < dim) {
//      val gaussian = new Gaussian(0, 1 / math.sqrt(targetDim))
//      val projMat = DenseMatrix.rand(targetDim, dim, gaussian)
//      projMatBC = sc.broadcast(projMat)
//    }

    // process SPDB samples
    var sampledData = sc.emptyRDD[Point]
    if (sampleSize == 1) {
      sampledData = sc.textFile(reader.getSampledFiles).repartition(1).map { record =>
          recordToPoint(dim, record)
      }
    } else {
      sampledData = sc.textFile(reader.getSampledFiles).map { record =>
        recordToPoint(dim, record)
      }
    }
    sampledData.cache()

    // calculated cutoff distance dc using sampling
    println("Computing cutoff distance......")
    val sampledPoints = sc.textFile(reader.sampleOneSPDB()).map { record =>
      recordToPoint(dim, record)
    }.collect
    val sampleNum = if (sampledPoints.length > 5000) 5000 else sampledPoints.length
    val distanceList = Array.ofDim[Double](sampleNum * (sampleNum - 1) / 2)
    var k = 0
    for (i <- 0 until sampleNum - 1) {
      for (j <- i + 1 until sampleNum) {
        distanceList(k) = sampledPoints(i).distance(sampledPoints(j))
        k += 1
      }
    }
    val sortedDis = distanceList.sorted
    val dc = sortedDis((sortedDis.length * pct).toInt)
    println("Cutoff distance(dc): " + dc)

    // local clustering
    val localClustersRDD = sampledData.mapPartitions { iter =>
      val block = new SPDataBlock(iter.toArray)
      block.localDPC(dc, windowSize, theta)
      block.clusters.iterator
    }
    localClustersRDD.cache()

    // merge local clusters and cluster assignment
    val localClusters = localClustersRDD.collect
    val combiner = new ClustersCombiner(localClusters, sampleSize)
    val mergeResult = combiner.mergeClusters()
    val mergeMapBC = sc.broadcast(mergeResult)
    localClustersRDD.mapPartitions { clusters =>
      val mergeMap = mergeMapBC.value
      val clusterResult = ArrayBuffer[String]()
      for (cluster <- clusters) {
        val label = mergeMap(cluster.uuid)
        for (pointID <- cluster.points) {
          clusterResult.append(s"$pointID,$label")
        }
      }
      clusterResult.iterator
    }.saveAsTextFile(outPath + "-sampled")

    // extract representative point
    val repMap = combiner.takeRepresentatives(numOfRep).view.mapValues(array => array.toSet).toMap
    val repIdSet = repMap.values.flatten.toSet
    val repPoints = sampledData.filter { point => repIdSet.contains(point.uid) }.collect
    for (repPoint <- repPoints) {
      breakable {
        for (label <- repMap.keys) {
          if (repMap(label).contains(repPoint.uid)) {
            repPoint.cluID = label
            break
          }
        }
      }
    }
    val repPointsBC = sc.broadcast(repPoints)

    // cluster assignment for unsample points
    if (sampleSize != numPart) {
      val unsampledFiles = reader.getUnsampledFiles
      val unsampledData = sc.textFile(unsampledFiles).map { record =>
        recordToPoint(dim, record)
      }
      unsampledData.mapPartitions { points =>
        val representatives = repPointsBC.value
        val clusterResult = ArrayBuffer[String]()
        for (point <- points) {
          var minDis = Double.MaxValue
          var label = -1
          for (rep <- representatives) {
            val dis = rep.distance(point)
            if (dis < minDis) {
              minDis = dis
              label = rep.cluID
            }
          }
          clusterResult.append(s"${point.uid},$label")
        }
        clusterResult.iterator
      }.saveAsTextFile(outPath + "-unsampled")
    }

    spark.close()
  }

  def saveResult(i: Int): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("DPSPC-SaveResult")
      .getOrCreate()
    val sc = spark.sparkContext
    // Aggregate all sample and non-sample clustering results and sort them by id
    val sampledResult = sc.textFile(outPath + "-sampled")
    var unsampledResult = sc.emptyRDD[String]
    if (sampleSize != numPart) {
      unsampledResult = sc.textFile(outPath + "-unsampled")
    }
    sampledResult.union(unsampledResult)
      .map(line => (line.split(",")(0).toLong, line))
      .sortByKey()
      .map(_._2)
      .repartition(1)
      .saveAsTextFile(outPath + "-" + i)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outPath + "-sampled"), true)
    if (sampleSize != numPart) {
      fs.delete(new Path(outPath + "-unsampled"), true)
    }
    spark.close()
  }

  private def recordToPoint(dim: Int, record: String): Point = {
    val strings = record.trim.split("\\|")
    val uid = strings(0).toLong
    val pValue = strings(1).split(",").slice(0, dim).map(_.toDouble)
//    if (projMat.rows != projMat.cols) {
//      val oldPValue = new DenseVector(pValue)
//      val newPValue = projMat * oldPValue
//      return new Point(newPValue.toArray, uid)
//    }
    new Point(pValue, uid)
  }

}

object DPSPC {
  def main(args: Array[String]): Unit = {
    var dpspc: DPSPC = null
    val startTime = System.nanoTime
    val inPath: String = args(0)
    val outPath: String = args(1)
    val dim: Int = args(2).toInt
    val numPart: Int = args(3).toInt
    if (args.length == 4) {
      dpspc = new DPSPC(inPath, outPath, dim, numPart)
      dpspc.runDPSPC()
    } else if (args.length == 9) {
      val sampleSize: Int = args(4).toInt
      val pct: Double = args(5).toDouble
      val windowSize: Int = args(6).toInt
      val theta: Double = args(7).toDouble
      val numOfRep: Int = args(8).toInt
      dpspc = new DPSPC(inPath, outPath, dim, numPart,
        sampleSize, pct, windowSize, theta, numOfRep)
      dpspc.runDPSPC()
    } else {
      println("Parameter number must be 4 or 9.")
    }
    val endTime = System.nanoTime
    val duration = endTime - startTime
    println(s"Code execution time: ${duration / 1e9d} seconds.")

    dpspc.saveResult(1)
  }
}
