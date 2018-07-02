package lyd.ai.dataflow.dbscan.util.commandLine

import lyd.ai.dataflow.dbscan.DbscanSettings

private [dbscan] trait EpsArg {
  var eps: Double = DbscanSettings.getDefaultEpsilon
}
