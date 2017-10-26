name := "LeukemiaGeneAnalysis"

version := "0.1"

scalaVersion := "2.11.4"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer
  )
}

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"