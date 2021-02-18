name := "Message"

version := "0.1"

scalaVersion := "2.12.9"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-avro" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.2.3" % Test
)

