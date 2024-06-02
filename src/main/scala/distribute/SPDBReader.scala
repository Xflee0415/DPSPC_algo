package distribute

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

class SPDBReader(sc: SparkContext, filePath: String) {
  private var files: Array[String] = Array.empty
  private var otherFiles: Array[String] = Array.empty

  def getSampledFiles: String = {
    files.mkString(",")
  }

  def getUnsampledFiles: String = {
    otherFiles.mkString(",")
  }

  def sampleSPDBs(numSamples: Int): Unit = {
    files = takeSPDBsFromHDFS(numSamples)._1
    otherFiles = takeSPDBsFromHDFS(numSamples)._2
  }

  def sampleOneSPDB(): String = {
    takeSPDBsFromHDFS(1)._1(0)
  }

  def countRecords(): Long = {
    sc.textFile(filePath).count()
  }

  private def takeSPDBsFromHDFS(numSamples: Int): (Array[String], Array[String]) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val directory = new Path(filePath)
    if (fs.exists(directory) && fs.isDirectory(directory)) {
      val allFiles = fs.listStatus(new Path(filePath))
        .filterNot(_.getPath.getName.startsWith("."))
        .filterNot(_.getPath.getName.startsWith("_"))
        .map(_.getPath.toString)
      val files = allFiles.take(numSamples)
      val otherFiles =allFiles.drop(numSamples)
      (files, otherFiles)
    } else {
      (Array.empty, Array.empty)
    }
  }

//  private def takeSPDBsLocal(numSamples: Int): (Array[String], Array[String]) = {
//    val directory = new File(filePath)
//    if (directory.exists && directory.isDirectory) {
//      val files = directory.listFiles
//        .filter(file => !file.isHidden && !file.getName.startsWith("_"))
//        .take(numSamples)
//        .map(file => "file://" + file.getAbsolutePath)
//      val otherFiles = directory.listFiles
//        .filter(file => !file.isHidden && !file.getName.startsWith("_"))
//        .drop(numSamples)
//        .map(file => "file://" + file.getAbsolutePath)
//      (files, otherFiles)
//    } else {
//      (Array.empty, Array.empty)
//    }
//  }
}
