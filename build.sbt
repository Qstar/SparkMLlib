import AssemblyKeys._

assemblySettings

name := "SparkMLlib"

libraryDependencies ++= Seq(
  "org.apache.spark"        %% "spark-core"               % "1.6.1",
  "org.apache.spark"        %% "spark-mllib"              % "1.6.1")
