package distribute

import org.apache.spark.Partitioner

class RandomPartitioner(val numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = key.hashCode() % numPartitions
}
