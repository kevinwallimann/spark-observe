# Spark-observe

Demo app for Spark 3.1.1 to demonstrate the new method `observe` in the dataset API (since Spark 3.0)

1. Compile with `mvn clean package`
2. Run in Batch-mode: `spark-submit target/spark-observe-0.1.0-SNAPSHOT.jar`
3. Run in Streaming-mode: `spark-submit --class com.github.kevinwallimann.SparkObserveStreaming target/spark-observe-0.1.0-SNAPSHOT.jar`
