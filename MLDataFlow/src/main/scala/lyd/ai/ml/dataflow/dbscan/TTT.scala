package lyd.ai.ml.dataflow.dbscan

import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
object TTT {
  def main(args: Array[String]) {
    val logFile = "D:\\code\\ML\\DBScan\\9_400K.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()

  }
}
