package MainGuide.ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object MinMaxScalerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MinMaxScalerExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "features")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(dataFrame)
    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
    scaledData.select("features", "scaledFeatures").show()

    sc.stop()
  }
}
