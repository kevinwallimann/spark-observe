package com.github.kevinwallimann
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.UUID


object SparkObserveBatch extends App {
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-observe Spark Job").getOrCreate()
    class SparkQueryExecutionListener extends QueryExecutionListener {

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        qe.observedMetrics.get("checkpoint1").foreach { row =>
          println(s"checkpoint1 rowCount: ${row.getAs[Long]("rowCount")}")
          println(s"checkpoint1 sum: ${row.getAs[Long]("sum")}")
          println(s"checkpoint1 sumAbs: ${row.getAs[Long]("sumAbs")}")
          println(s"checkpoint1 crc32: ${row.getAs[Long]("crc32")}")
        }
        qe.observedMetrics.get("checkpoint2").foreach { row =>
          println(s"checkpoint2 rowCount: ${row.getAs[Long]("rowCount")}")
          println(s"checkpoint2 sum: ${row.getAs[Long]("sum")}")
          println(s"checkpoint2 sumAbs: ${row.getAs[Long]("sumAbs")}")
          println(s"checkpoint2 crc32: ${row.getAs[Long]("crc32")}")
        }
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    val queryExecutionListener = new SparkQueryExecutionListener
    spark.listenerManager.register(queryExecutionListener)

    val uuid = UUID.randomUUID().toString

    import spark.implicits._
    val df = (1 to 100).map(_ * -1).toDF.
      withColumn("crc32value", crc32(col("value").cast("String"))).
      observe("checkpoint1",
        count(lit(1)).as("rowCount"),
        // countDistinct(col("value")).as("distinctCount"), // distinct aggregates are not allowed
        sum(col("value")).as("sum"),
        sum(abs(col("value"))).as("sumAbs"),
        sum(col("crc32value")).as("crc32")
      ).
      filter("value % 2 == 0").
      observe("checkpoint2",
        count(lit(1)).as("rowCount"),
        // countDistinct(col("value")).as("distinctCount"), // distinct aggregates are not allowed
        sum(col("value")).as("sum"),
        sum(abs(col("value"))).as("sumAbs"),
        sum(col("crc32value")).as("crc32")
      )
    df.write.
      parquet(s"/tmp/out-batch/$uuid")


    // df.explain
  }
}
