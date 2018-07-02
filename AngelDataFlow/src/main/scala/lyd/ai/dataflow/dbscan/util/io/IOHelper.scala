package lyd.ai.dataflow.dbscan.util.io

import org.apache.spark.SparkContext
import scala.collection.mutable.WrappedArray.ofDouble
import lyd.ai.dataflow.dbscan.{DbscanModel, RawDataSet, ClusterId, PointCoordinates}
import org.apache.spark.rdd.RDD
import lyd.ai.dataflow.dbscan.spatial.Point

/** Contains functions for reading and writing data
  *
  */
object IOHelper {


  def readDataset (sc: SparkContext, path: String): RawDataSet = {
    val rawData = sc.textFile (path)

    rawData.map (
      line => {
        new Point (line.split(separator).map( _.toDouble ))
      }
    )
  }

  def saveClusteringResult (model: DbscanModel, outputPath: String) {

    model.allPoints.map ( pt => {

      pt.coordinates.mkString(separator) + separator + pt.clusterId
    } ).saveAsTextFile(outputPath)
  }

  private [dbscan] def saveTriples (data: RDD[(Double, Double, Long)], outputPath: String) {
    data.map ( x => x._1 + separator + x._2 + separator + x._3 ).saveAsTextFile(outputPath)
  }

  private def separator = ","

}
