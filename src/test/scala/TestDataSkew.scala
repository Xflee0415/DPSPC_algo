import distribute.SPDBGenerator
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class TestDataSkew {
  @Test
  def testPartitionSize(): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RSPTest")
      .getOrCreate()
    val sc = spark.sparkContext
    val inPath = "D:/work/data/test/test_skew/dim128_concat.txt"
    val numPart = 4
    // 根据输入数据路径生成对应的SPDB文件
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val rspPath = inPath.stripSuffix(".txt") + "-rsp"
    if (!fs.exists(new Path(rspPath))) {
      SPDBGenerator.generateSPDB(sc, inPath, rspPath, numPart)
    }
    // 获取每个分区数据量并输出
    val partitionSizes = sc.textFile(rspPath).mapPartitionsWithIndex { (index, iterator) =>
      Iterator((index, iterator.size))
    }.collect()
    partitionSizes.foreach { case (index, size) =>
      println(s"Partition $index has size $size")
    }
  }

}
