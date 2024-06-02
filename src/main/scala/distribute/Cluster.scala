package distribute

import java.util.UUID

case class Cluster (centroid: Point,
                    points: Array[Long],
                    var label: Int,
                    uuid: String = UUID.randomUUID().toString,
                    var visited: Boolean = false)
