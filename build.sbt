name := "LeukemiaGeneAnalysis"

version := "0.1"

scalaVersion := "2.11.4"

//Add Spark for multicluster computation
libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer
  )
}

libraryDependencies += "com.github.haifengl" %% "smile-scala" % "1.5.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"