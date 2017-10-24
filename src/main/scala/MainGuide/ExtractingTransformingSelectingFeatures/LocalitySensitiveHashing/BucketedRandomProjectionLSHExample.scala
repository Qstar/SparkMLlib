package MainGuide.ExtractingTransformingSelectingFeatures.LocalitySensitiveHashing

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors

object BucketedRandomProjectionLSHExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("BucketedRandomProjectionLSHExample")
      .getOrCreate()

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "keys")

    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "keys")

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = brp.fit(dfA)

    // Feature Transformation
    model.transform(dfA).show(false)
    // Cache the transformed columns
    val transformedA = model.transform(dfA).cache()
    val transformedB = model.transform(dfB).cache()

    // Approximate similarity join
    model.approxSimilarityJoin(dfA, dfB, 1.5).show(false)
    model.approxSimilarityJoin(transformedA, transformedB, 1.5).show(false)
    // Self Join
    model.approxSimilarityJoin(dfA, dfA, 2.5).filter("datasetA.id < datasetB.id").show(false)

    // Approximate nearest neighbor search
    model.approxNearestNeighbors(dfA, key, 2).show(false)
    model.approxNearestNeighbors(transformedA, key, 2).show(false)

    spark.stop()
  }
}
