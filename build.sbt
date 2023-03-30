name := "education-spark"

scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"                 % "3.3.2",
  "org.apache.spark" %% "spark-sql"                  % "3.3.2",
  "org.apache.spark" %% "spark-streaming"            % "3.3.2",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10"       % "3.3.2",
  "org.apache.kafka"  % "kafka-clients"              % "3.4.0"
)

enablePlugins(JavaAppPackaging)
