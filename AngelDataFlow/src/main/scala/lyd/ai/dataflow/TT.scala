package lyd.ai.dataflow

import lyd.ai.dataflow.dbscan.spatial.Point
import lyd.ai.dataflow.dbscan.{Dbscan, DbscanSettings}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import lyd.ai.dataflow.dbscan.util.io.IOHelper
//https://github.com/alitouka/spark_dbscan/wiki/How-It-Works
object DBScan {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("dbscan").setMaster("local");
    val sc = new SparkContext(conf);
    val data = IOHelper.readDataset(sc, "D:\\\\code\\\\ML\\\\DBScan\\\\9_400K.csv")
    val clusteringSettings = new DbscanSettings ().withEpsilon(25).withNumberOfPoints(30)
    val model = Dbscan.train (data, clusteringSettings)
    model.clusteredPoints.foreach(print)
    val predictedClusterId = model.predict(new Point (100, 100))
    println (predictedClusterId)
  }
}
