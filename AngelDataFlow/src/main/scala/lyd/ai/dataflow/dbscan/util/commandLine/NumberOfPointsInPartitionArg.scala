package lyd.ai.dataflow.dbscan.util.commandLine

import lyd.ai.dataflow.dbscan.spatial.rdd.PartitioningSettings


private [dbscan] trait NumberOfPointsInPartitionArg {
  var numberOfPoints: Long = PartitioningSettings.DefaultNumberOfPointsInBox
}
