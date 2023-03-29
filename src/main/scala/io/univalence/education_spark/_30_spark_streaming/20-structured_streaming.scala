package io.univalence.education_spark._30_spark_streaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types._

object _20_structured_streaming {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("StructuredStreamingApp")
        .master("local[*]")
        .config("spark.sql.streaming.checkpointLocation", "data/output/checkpoint")
        .getOrCreate()

    import spark.implicits._

    val rawDf: DataFrame =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", Configuration.bootstrapServers)
        .option("subscribe", Configuration.inputTopic)
        .load()

    val orderSchema =
      StructType(
        Seq(
          StructField("id", StringType),
          StructField("client", StringType),
          StructField("timestamp", TimestampType),
          StructField("product", StringType),
          StructField("price", DoubleType)
        )
      )

    val df =
      rawDf
        .select($"key".cast("STRING").as("key"), $"value".cast("STRING").as("value"))
        .select($"key", from_csv($"value", orderSchema, Map.empty[String, String]).as("value"))

    df.printSchema()

    val resultDf =
      df
        .groupBy($"value.product".as("key"))
        .agg(count(lit(1)).as("count"))
        .select($"key", $"count".cast("STRING").as("value"))

    val ds: StreamingQuery =
      resultDf.writeStream
        .outputMode(OutputMode.Update())
        .format("kafka")
        .option("kafka.bootstrap.servers", Configuration.bootstrapServers)
        .option("topic", Configuration.outputTopic)
        .start()

    ds.awaitTermination()
  }

}


