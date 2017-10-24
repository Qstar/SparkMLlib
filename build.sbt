import AssemblyKeys._

assemblySettings

name := "SparkMLlib"

libraryDependencies ++= Seq(
  "org.apache.spark"        %% "spark-core"               % "2.1.1",
  "org.apache.spark"        %% "spark-mllib"              % "2.1.1")
