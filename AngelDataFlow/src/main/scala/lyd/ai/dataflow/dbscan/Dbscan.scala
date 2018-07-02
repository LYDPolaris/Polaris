package lyd.ai.dataflow.dbscan

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.ml.distance.{EuclideanDistance, DistanceMeasure}
import scala.reflect._
import org.apache.spark.internal.Logging
import lyd.ai.dataflow.dbscan.spatial.DistanceAnalyzer
import lyd.ai.dataflow.dbscan.spatial.rdd.PartitioningSettings

abstract class Dbscan protected (
  protected val settings: DbscanSettings,
  protected val partitioningSettings: PartitioningSettings = new PartitioningSettings ())
  extends Serializable
  with Logging {

  protected val distanceAnalyzer = new DistanceAnalyzer(settings)

  protected def run (data: RawDataSet): DbscanModel
}

/** Serves as a factory for objects which implement the DBSCAN algorithm
  * and provides a convenience method for starting the algorithm
  *
  */
object Dbscan {


  protected def apply (settings: DbscanSettings,
    partitioningSettings: PartitioningSettings = new PartitioningSettings ()): Dbscan = {

    new DistributedDbscan(settings, partitioningSettings)
  }


  def train (data: RawDataSet, settings: DbscanSettings,
    partitioningSettings: PartitioningSettings = new PartitioningSettings ()): DbscanModel = {
    Dbscan (settings, partitioningSettings).run (data)
  }

  private [dbscan] def keepOnlyPairsWithKeys [K, V] (pairs: RDD[(K, V)], keysToLeave: RDD[K])
    (implicit arg0: ClassTag[K], arg1: ClassTag[V]) = {

    keysToLeave.map ( (_, null) ).join (pairs).map ( x => (x._1, x._2._2) )
  }



}
