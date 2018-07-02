package lyd.ai.dataflow.dbscan.exploratoryAnalysis

import lyd.ai.dataflow.dbscan.spatial.Point

private [dbscan] class PointWithDistanceToNearestNeighbor (pt: Point, d: Double = Double.MaxValue) extends  Point (pt) {
  var distanceToNearestNeighbor = d
}
