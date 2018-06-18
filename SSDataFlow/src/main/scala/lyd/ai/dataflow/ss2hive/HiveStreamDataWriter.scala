/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lyd.ai.dataflow.ss2hive

import java.util.{List => JList, Map => JMap}
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods._
import lyd.ai.dataflow.ss2hive.common.{CachedHiveWriters, CachedKey, HiveOptions, HiveWriter}
import lyd.ai.dataflow.ss2hive.utils.Logging

case object HiveStreamWriterCommitMessage extends WriterCommitMessage

class HiveStreamDataWriter(
   partitionId: Int,
   attemptNumber: Int,
   columnName: Seq[String],
   partitionCols: Seq[String],
   dataSourceOptionsMap: JMap[String, String],
   initClassLoader: ClassLoader,
   isolatedClassLoader: ClassLoader) extends DataWriter[Row] with Logging {

  private implicit def formats = DefaultFormats

  private val hiveOptions =
    HiveOptions.fromDataSourceOptions(new DataSourceOptions(dataSourceOptionsMap))
  private val ugi = hiveOptions.getUGI()

  private val inUseWriters = new mutable.HashMap[CachedKey, HiveWriter]()

  private val executorService = Executors.newSingleThreadScheduledExecutor()
  executorService.scheduleAtFixedRate(new Runnable {
    Thread.currentThread().setContextClassLoader(isolatedClassLoader)

    override def run(): Unit = {
      inUseWriters.foreach(_._2.heartbeat())
    }
  }, 10L, 10L, TimeUnit.SECONDS)

  private def withClassLoader[T](func: => T): T = {
    try {
      Thread.currentThread().setContextClassLoader(isolatedClassLoader)
      func
    } finally {
      Thread.currentThread().setContextClassLoader(initClassLoader)
    }
  }

  override def write(row: Row): Unit = withClassLoader {
    val partitionValues = partitionCols.map { col => stringfyField(row.get(row.fieldIndex(col))) }
    val hiveEndPoint =
      Class.forName("org.apache.hive.hcatalog.streaming.HiveEndPoint", true, isolatedClassLoader)
      .getConstructor(classOf[String], classOf[String], classOf[String], classOf[JList[String]])
      .newInstance(
        hiveOptions.metastoreUri, hiveOptions.dbName, hiveOptions.tableName, partitionValues.asJava)
      .asInstanceOf[Object]

    val key = CachedKey(
      hiveOptions.metastoreUri, hiveOptions.dbName, hiveOptions.tableName, partitionValues)

    def getNewWriter(): HiveWriter = {
      val writer = CachedHiveWriters.getOrCreate(
        key, hiveEndPoint, hiveOptions, ugi, isolatedClassLoader)
      writer.beginTransaction()
      writer
    }
    val writer = inUseWriters.getOrElseUpdate(key, {
      logDebug(s"writer for $key not found in local cache")
      getNewWriter()
    })

    val jRow = Extraction.decompose(rowToMap(columnName, row))
    val jString = compact(render(jRow))

    logDebug(s"Write JSON row ${pretty(render(jRow))} into Hive Streaming")
    writer.write(jString.getBytes("UTF-8"))

    if (writer.totalRecords() >= hiveOptions.batchSize) {
      writer.commitTransaction()
      writer.beginTransaction()
    }
  }

  override def abort(): Unit = withClassLoader {
    inUseWriters.foreach { case (key, writer) =>
      writer.abortTransaction()
      CachedHiveWriters.recycle(writer)
      logDebug(s"Recycle writer $writer for $key to global cache")
    }
    inUseWriters.clear()
    executorService.shutdown()
  }

  override def commit(): WriterCommitMessage = withClassLoader {
    inUseWriters.foreach { case (key, writer) =>
      writer.commitTransaction()
      CachedHiveWriters.recycle(writer)
      logDebug(s"Recycle writer $writer for $key to global cache")
    }
    inUseWriters.clear()
    executorService.shutdown()

    HiveStreamWriterCommitMessage
  }

  private def stringfyField(col: Any): String = {
    col match {
      case _: Array[Byte] =>
        throw new UnsupportedOperationException("Cannot convert partition column with BinaryType " +
          "to String")
      case _: Seq[_] =>
        throw new UnsupportedOperationException("Cannot convert partition column with ArrayType " +
          "to String")
      case _: Map[_, _] =>
        throw new UnsupportedOperationException("Cannot convert partition column with MapType " +
          "to String")
      case _: Row =>
        throw new UnsupportedOperationException("Cannot convert partition column with StructType " +
          "to String")
      case i => i.toString
    }
  }

  private def rowToMap(columnName: Seq[String], row: Row): Map[String, Any] = {
    columnName.map { col =>
      val field = row.get(row.fieldIndex(col)) match {
        case b: java.math.BigDecimal => new BigDecimal(b)
        case r: Row => rowToMap(r.schema.map(_.name), r)
        case e => e
      }
      col -> field
    }.toMap
  }
}