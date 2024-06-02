//import distribute.{Point, SPDataBlock}
//import org.junit.Test
//
//import java.io.PrintWriter
//import scala.io.Source
//import scala.util.Random
//
//class TestLocal {
//  val filePath: String ="D:\\work\\data\\test\\test_gaussian_sample\\test_gaussian_data.txt"
//  val outPath: String = "D:\\work\\data\\test\\test_gaussian_sample\\result.txt"
//  val dim: Int = 10
//  val delim: String = ","
//  val pct: Double = 0.02
//  val projVecNum: Int = 3
//  val windowSize: Int = 10
//  val theta: Double = 2e-4
//
//  def readPointsFromFile(): Array[Point] = {
//    val source = Source.fromFile(filePath)
//    try {
//      source.getLines().map { line =>
//        val strings = line.trim.split("\\|")
//        val uid = strings(0).toLong
//        val pValue = strings(1).split(delim).slice(0, dim).map(_.toDouble)
//        new Point(pValue, uid)
//      }.toArray
//    } finally {
//      source.close()
//    }
//  }
//
//  @Test
//  def localCluster(): Unit = {
//    val startTime = System.currentTimeMillis()
//
//    val points = readPointsFromFile()
//
//    val indices = points.indices.toArray
//    val sampleSize = 5000
//    val sampledPoints = Random.shuffle(indices.toList).take(sampleSize).map(i => points(i)).toArray
//    val distanceList = Array.ofDim[Double](sampleSize * (sampleSize - 1) / 2)
//    var k = 0
//    for (i <- 0 until sampleSize - 1) {
//      for (j <- i + 1 until sampleSize) {
//        distanceList(k) = sampledPoints(i).distance(sampledPoints(j))
//        k += 1
//      }
//    }
//    val sortedDis = distanceList.sorted
//    val dc = sortedDis((sortedDis.length * pct).toInt)
//
//    val block = new SPDataBlock(points)
//    block.localDPC(dc, projVecNum, windowSize, theta, Random.nextLong())
//
//    val endTime = System.currentTimeMillis()
//    val executionTime = endTime - startTime
//    println(s"Execution Time: ${executionTime / 1000.0} seconds")
//
//    println("Writing result to file......")
//    val clusters = block.clusters
//    val writer = new PrintWriter(outPath)
//    try {
//      for (cluster <- clusters) {
//        for (pid <- cluster.points) {
//          writer.println(s"$pid,${cluster.label}")
//        }
//      }
//    } finally {
//      writer.close()
//    }
//  }
//
//}
