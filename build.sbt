name := "spark_course"

scalaVersion := "2.13.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value
)
