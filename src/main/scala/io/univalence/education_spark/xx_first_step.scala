package io.univalence.education_spark

import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}

object xx_first_step {
  def main(args: Array[String]): Unit = {
    val historyDir = "data/target/spark-events"
    Files.createDirectories(Paths.get(historyDir))

    val spark =
      SparkSession
        .builder()
        .appName("first-step")
        .master("local[*]")
//        .master("spark://localhost:7077")
        .config("spark.eventLog.enabled", true)
        .config("spark.eventLog.dir", historyDir)
        .getOrCreate()

    val filename = "/opt/spark-data/threetriangle/venues.txt.gz"
    val dataframe =
      spark.read
        .csv(filename)

    dataframe.show()

//    dataframe.limit(20).write.csv("/opt/spark-data/out.csv")
  }
}
