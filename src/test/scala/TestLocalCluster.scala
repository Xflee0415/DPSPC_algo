//import distribute.{Point, SPDataBlock}
//import org.junit.Test
//
//import scala.collection.mutable.ArrayBuffer
//import scala.io.Source
//import scala.util.Random
//
//class TestLocalCluster {
//
//  @Test
//  def testLocalCluster(): Unit = {
//    val filePath = "D:\\work\\data\\test\\test_dpspc\\test_data-rsp\\part-00003"
//    val source = Source.fromFile(filePath)
//    val points = ArrayBuffer[Point]()
//    try {
//      // 获取文件内容并逐行处理
//      val lines = source.getLines()
//      for (line <- lines) {
//        val strings = line.trim.split("\\|")
//        val uid = strings(0).toLong
//        val pValue = strings(1).split(",").slice(0, 10).map(_.toDouble)
//        points.append(new Point(pValue, uid))
//      }
//    } finally {
//      // 确保关闭文件流
//      source.close()
//    }
//    val block = new SPDataBlock(points.toArray)
//    block.localDPC(4.123, 5, 20, 1e-3, Random.nextLong())
//    val clusters = block.clusters
//    println(clusters)
//  }
//}
