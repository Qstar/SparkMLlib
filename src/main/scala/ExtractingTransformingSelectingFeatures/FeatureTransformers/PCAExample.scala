package ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PCAExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PCAExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("PCAFeatures")
      .setK(3)
      .fit(df)

    val result = pca.transform(df).select("PCAFeatures")
    result.show(false)

    sc.stop()
  }
}
