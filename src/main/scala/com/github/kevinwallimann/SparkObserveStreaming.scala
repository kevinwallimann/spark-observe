package com.github.kevinwallimann

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener}
import org.apache.spark.sql.types.{IntegerType, StructType}

import java.util.UUID

object SparkObserveStreaming extends App {
  val spark = SparkSession.builder().appName("spark-observe Spark Job").getOrCreate()

  class SparkQueryExecutionListener extends StreamingQueryListener {
    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      println(s"BatchId: ${event.progress.batchId}")
      Option(event.progress.observedMetrics.get("checkpoint1")).foreach(row => {
        println(s"Checkpoint 1 listCollect: ${row.getAs[Seq[Int]]("listCollect")}")
        println(s"Checkpoint 1 rowCount: ${row.getAs[Long]("rowCount")}")
        println(s"Checkpoint 1 sum: ${row.getAs[Long]("sum")}")
        println(s"Checkpoint 1 sumAbs: ${row.getAs[Long]("sumAbs")}")
        println(s"Checkpoint 1 crc32: ${row.getAs[Long]("crc32")}")
      })
      Option(event.progress.observedMetrics.get("checkpoint2")).foreach(row => {
        println(s"Checkpoint 2 listCollect: ${row.getAs[Seq[Int]]("listCollect")}")
        println(s"Checkpoint 2 rowCount: ${row.getAs[Long]("rowCount")}")
        println(s"Checkpoint 2 sum: ${row.getAs[Long]("sum")}")
        println(s"Checkpoint 2 sumAbs: ${row.getAs[Long]("sumAbs")}")
        println(s"Checkpoint 2 crc32: ${row.getAs[Long]("crc32")}")    })
    }

    def onQueryStarted(event: QueryStartedEvent): Unit = {}
    def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
  }

  val queryExecutionListener = new SparkQueryExecutionListener
  spark.streams.addListener(queryExecutionListener)

  val uuid = UUID.randomUUID().toString

  val schemaCatalyst = new StructType().
    add("key", IntegerType, nullable = true).
    add("value", IntegerType, nullable = true)
  val collectListDeterministic = new Column(CollectListDeterministic(col("key").expr).
    toAggregateExpression(isDistinct = false))
  val input = MemoryStream[Row](42, spark.sqlContext)(RowEncoder(schemaCatalyst))
  val df = input.toDF().
    withColumn("crc32value", crc32(col("value").cast("String"))).
    observe("checkpoint1",
      count(lit(1)).as("rowCount"),
      collectListDeterministic.as("listCollect"),
      // countDistinct(col("value")).as("distinctCount"), // distinct aggregates are not allowed
      sum(col("value")).as("sum"),
      sum(abs(col("value"))).as("sumAbs"),
      sum(col("crc32value")).as("crc32")
    ).
    filter("value % 2 == 0").
    observe("checkpoint2",
      count(lit(1)).as("rowCount"),
      collectListDeterministic.as("listCollect"),
      // countDistinct(col("value")).as("distinctCount"), // distinct aggregates are not allowed
      sum(col("value")).as("sum"),
      sum(abs(col("value"))).as("sumAbs"),
      sum(col("crc32value")).as("crc32")
    ).
    writeStream.
    format("parquet").
    outputMode(OutputMode.Append()).
    option("checkpointLocation", s"/tmp/out-streaming/$uuid/checkpoint-location")
  val query = df.start(s"/tmp/out-streaming/$uuid/bla")
  input.addData(generateData(1, 100))
  query.processAllAvailable()
  input.addData(generateData(101, 300))
  query.processAllAvailable()
  input.addData(generateData(301, 600))
  query.processAllAvailable()
  query.stop()

  private def generateData(start: Int, end: Int): Seq[Row] = {
    val keys = (start to end).map(_ % 2)
    val value = start to end
    keys.zip(value).map(a => Row(a._1, a._2))
  }
}
