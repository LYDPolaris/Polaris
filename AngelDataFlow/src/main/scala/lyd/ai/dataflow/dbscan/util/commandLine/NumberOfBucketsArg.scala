package lyd.ai.dataflow.dbscan.util.commandLine

import lyd.ai.dataflow.dbscan.exploratoryAnalysis.ExploratoryAnalysisHelper

private [dbscan] trait NumberOfBucketsArg {
  var numberOfBuckets: Int = ExploratoryAnalysisHelper.DefaultNumberOfBucketsInHistogram
}
