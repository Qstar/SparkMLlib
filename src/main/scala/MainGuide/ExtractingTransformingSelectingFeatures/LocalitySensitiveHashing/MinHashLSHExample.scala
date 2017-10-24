package MainGuide.ExtractingTransformingSelectingFeatures.LocalitySensitiveHashing

import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object MinHashLSHExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MinHashLSHExample")
      .getOrCreate()

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "keys")

    val dfB = spark.createDataFrame(Seq(
      (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
      (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
      (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "keys")

    val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))

    val mh = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = mh.fit(dfA)

    // Feature Transformation
    model.transform(dfA).show(false)
    // Cache the transformed columns
    val transformedA = model.transform(dfA).cache()
    val transformedB = model.transform(dfB).cache()

    // Approximate similarity join
    model.approxSimilarityJoin(dfA, dfB, 0.6).show(false)
    model.approxSimilarityJoin(transformedA, transformedB, 0.6).show(false)
    // Self Join
    model.approxSimilarityJoin(dfA, dfA, 0.6).filter("datasetA.id < datasetB.id").show(false)

    // Approximate nearest neighbor search
    model.approxNearestNeighbors(dfA, key, 2).show(false)
    model.approxNearestNeighbors(transformedA, key, 2).show(false)

    spark.stop()
  }
}
