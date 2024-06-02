package distribute

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkContext

object SPDBGenerator extends Serializable {
  def generateSPDB(sc: SparkContext,
                   inPath: String,
                   outPath: String,
                   numParts: Int): Unit = {
    println("Generating SPDB files......")
    sc.textFile(inPath).mapPartitions{ iter => {
      val list = Random.shuffle(iter.toList)
      val result = new ArrayBuffer[(Int, String)]()
      for (i <- list.indices) {
        result.append((i, list(i)))
      }
      result.iterator
    }}.partitionBy(new RandomPartitioner(numParts))
      .values.saveAsTextFile(outPath)
    println("Generate SPDB successfully!")
  }
}
