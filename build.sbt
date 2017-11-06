import AssemblyKeys._

assemblySettings

name := "SparkMLlib"

libraryDependencies ++= Seq(
  "org.apache.spark"        %% "spark-core"               % "2.2.0",
  "org.apache.spark"        %% "spark-mllib"              % "2.2.0")
