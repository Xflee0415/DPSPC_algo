package util

import java.io.{File, PrintWriter}
import scala.io.Source

case class PointInfo(uid: String, var values: Array[Double], cid: String)

object Normalization {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: scala DataNormalization.scala input_file output_file")
    } else {
      val inputFile = args(0)
      val outputFile = args(1)

      val dataset = readDataset(inputFile)
      if (dataset.nonEmpty) {
        normalizeColumns(dataset)
        writeDataset(outputFile, dataset)
        println(s"Normalized dataset written to $outputFile")
      }
    }
  }

  private def readDataset(fileName: String): Array[PointInfo] = {
    val source = Source.fromFile(fileName)
    val lines = source.getLines().toArray
    val result = Array.ofDim[PointInfo](lines.length)
    for (i <- lines.indices) {
      val strs = lines(i).trim.split("\\|")
      val uid = strs(0)
      val cid = strs(2)
      val values = strs(1).split(",").map(_.toDouble)
      result(i) = PointInfo(uid, values, cid)
    }
    result
  }

  private def normalizeColumns(dataset: Array[PointInfo]): Unit = {
    val numColumns = dataset(0).values.length
    val minValues = Array.fill[Double](numColumns) {Double.MaxValue}
    val maxValues = Array.fill[Double](numColumns) {Double.MinValue}

    // Find min and max values for each column
    for (point <- dataset) {
      for (i <- 0 until numColumns) {
        minValues(i) = math.min(minValues(i), point.values(i))
        maxValues(i) = math.max(maxValues(i), point.values(i))
      }
    }

    // Normalize each column
    for (point <- dataset) {
      val normalizedRow = new Array[Double](numColumns)
      for (i <- 0 until numColumns) {
        normalizedRow(i) = (point.values(i) - minValues(i)) / (maxValues(i) - minValues(i))
      }
      point.values = normalizedRow
    }
  }

  private def writeDataset(fileName: String, dataset: Array[PointInfo]): Unit = {
    val pw = new PrintWriter(new File(fileName))
    for (row <- dataset) {
      pw.println(row.uid + "|" + row.values.mkString(",") + "|" + row.cid)
    }
    pw.close()
  }
}
