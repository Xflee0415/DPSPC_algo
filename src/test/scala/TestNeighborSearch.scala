import org.junit.Test
import smile.math.matrix.Matrix
import smile.neighbor.{KDTree, LinearSearch, Neighbor}

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

class TestNeighborSearch {
  @Test
  def testRange(): Unit = {
    println("range 0.5")
    val data = Matrix.randn(1000, 10).toArray
    val kdtree = new KDTree[Array[Double]](data, data)
    val list = new util.ArrayList[Neighbor[Array[Double], Array[Double]]]()
    val queryPoint = data(100)
    kdtree.range(queryPoint, 5, list)
    println("list size: " + list.size())
    val list1 = list.asScala.toList
    for (neighbor <- list1) {
      println("key: " + neighbor.key.mkString(","))
      println("value: " + neighbor.value.mkString(","))
      println("distance: " + neighbor.distance)
    }
  }

}
