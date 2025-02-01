organization := "spark-wtf"
name := "i-like-it-when-a-plan-comes-together"
version := "1.0-SNAPSHOT"

scalaVersion := "2.12.17"
lazy val sparkVersion = "3.5.4"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

