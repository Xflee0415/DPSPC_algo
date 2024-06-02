package distribute

import scala.math.sqrt

class Point(val vector: Array[Double], val norm: Double, val uid: Long, var id: Int, var supID: Int,
            var rho: Double, var delta: Double, var cluID: Int, var isDP: Boolean) extends Serializable {

  var dmax: Double = 0

  def this(vector: Array[Double], uid: Long) =
    this(vector, Point.calcNorm(vector), uid, -1, -1, 0, Double.MaxValue, 0, false)

  def distance(other: Point): Double = {
    require(this.vector.length == other.vector.length, "Points must have the same dimension")
    val sumSquaredNorm = this.norm * this.norm + other.norm * other.norm
    val dot = this.vector.zip(other.vector).map { case (a, b) => a * b }.sum
    val diff = sumSquaredNorm - 2 * dot
    math.sqrt(if (diff < 0) 0 else diff)
  }

}

object Point{
  private def calcNorm(vector: Array[Double]): Double = {
    sqrt(vector.map(x => x * x).sum)
  }
}
