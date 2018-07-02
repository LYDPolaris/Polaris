package lyd.ai.dataflow.dbscan.spatial

import lyd.ai.dataflow.dbscan._

private [dbscan] class BoxIdGenerator (val initialId: BoxId) {
  var nextId = initialId

  def getNextId (): BoxId = {
    nextId += 1
    nextId
  }
}
